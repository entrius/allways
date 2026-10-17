"""Runs the existing swap-loop decision tests against the new decision core.

ParityLoop has SolanaSwapLoop's constructor and ``decide``, but decides with ``swaps.decide`` over a
context that answers synchronously from the same fakes the old tests use: so every old expectation is a
check on the new rules."""

from pathlib import Path
from typing import Any

from allways.assets.asset import ProviderUnreachableError
from allways.solana.client import swap_key_from_tx_hash
from allways.validator.solana_swap_loop import (
    SolanaSwapLoop,
    SwapAction,
    SwapDecision,
    collateral_verdict_for,
    payout_reuse_reason,
)
from allways.validator_v2.actuator.swaps import Leg, NotReady, decide


class LegacyContext:
    def __init__(self, loop: 'ParityLoop', now: int) -> None:
        self.loop, self.now = loop, now
        self.legs = {}  # chain → Leg, as the production context records them for the status report

    def provider(self, chain):
        return self.loop.providers.get(chain)

    def reservation(self, swap):
        try:
            return self.loop.client.get_reservation(swap.miner, str(swap.collateral_chain))
        except Exception:
            return None

    def leg(self, chain, tx_hash, recipient, amount, block_hint, sender):
        provider = self.provider(chain)
        if provider is None:
            return Leg('down')
        tx_hash = provider.chain.canonical_tx_hash(tx_hash)
        if tx_hash is None:
            return Leg('no')
        try:
            info = provider.verify_transaction(
                tx_hash=tx_hash,
                expected_recipient=recipient,
                expected_amount=amount,
                block_hint=block_hint,
                expected_sender=sender or None,
            )
        except ProviderUnreachableError:
            return Leg('down')
        leg = Leg('no') if info is None else Leg('ok' if info.confirmed else 'pending', info)
        self.legs[chain] = leg
        return leg

    def cancel_evidence(self, swap, amount):
        provider = self.provider(swap.to_chain)
        if provider is None:
            return None
        try:
            return provider.cancel_evidence(
                swap.user_to_addr,
                amount,
                getattr(swap, 'to_tx_hash', '') or None,
                from_address=getattr(swap, 'miner_to_addr', '') or None,
            )
        except Exception:
            return None

    def delivery_refused(self, swap):
        provider = self.provider(swap.to_chain)
        if provider is None:
            return False
        try:
            return provider.delivery_refused(swap.user_to_addr, int(swap.initiated_at))
        except Exception:
            return True

    def collateral_ok(self, swap, reservation):
        try:
            return collateral_verdict_for(
                self.loop.state_store,
                swap.miner,
                str(swap.collateral_chain),
                swap.from_chain,
                int(swap.from_amount),
                swap.to_chain,
                int(swap.to_amount),
                int(swap.collateral_amount),
                int(reservation.created_at),
                self.loop.providers,
            )
        except (ProviderUnreachableError, ValueError) as e:
            raise NotReady(f'alpha leg unpriceable at fill block: {e}')

    def deposit_claimed(self, swap):
        store = self.loop.state_store
        if store is None:
            return False
        key = swap_key_from_tx_hash(swap.from_tx_hash).hex()
        store.record_deposit_claim(swap.from_tx_hash, key, self.now)
        held_key, initiated = store.deposit_claim(swap.from_tx_hash)
        return held_key != key or initiated

    def payout_reuse(self, swap):
        provider = self.provider(swap.from_chain)
        if provider is None:
            return False
        return (
            payout_reuse_reason(
                self.loop.state_store,
                provider.chain,
                swap.from_chain,
                swap.user_from_addr,
                swap.miner_from_addr,
                swap.from_tx_hash,
            )
            is not None
        )

    def claim_delivery(self, swap):
        store = self.loop.state_store
        if store is None:
            return True
        return store.claim_delivery(
            str(swap.miner),
            swap.to_tx_hash,
            swap.swap_key.hex() if isinstance(swap.swap_key, bytes) else str(swap.swap_key),
            self.now,
        )

    def owes_settlement(self, swap):
        backing = str(getattr(swap, 'collateral_chain', '') or '').lower()
        if self.loop.relay is None or not backing or backing == 'sol':
            return False
        return self.loop.relay.has_pending_debit(str(swap.miner))


class ParityLoop(SolanaSwapLoop):
    """SolanaSwapLoop whose ``decide`` is the new core; everything else is inherited for the fixtures."""

    def decide(self, swap: Any, now: int) -> SwapAction:
        status = swap.status if isinstance(swap.status, str) else type(swap.status).__name__
        if status in ('Active', 'Fulfilled') and self.state_store is not None:
            key = swap_key_from_tx_hash(swap.from_tx_hash).hex()
            self.state_store.record_deposit_claim(swap.from_tx_hash, key, now)
            self.state_store.mark_deposit_initiated(key)
        ctx = LegacyContext(self, now)
        action = decide(swap, ctx, now, self.fee_divisor)
        for chain, leg in ctx.legs.items():
            self._publish_confs(swap, 'source' if chain == swap.from_chain else 'dest', chain, leg.info)
        if action.decision.value == 'reject':
            self._reject_logged(swap, action.reason)
        return SwapAction(SwapDecision(action.decision.value), action.target_at, action.reason, action.reason_code)


# ── floor sweep and stake-weight vote ───────────────────────────────────────

from types import SimpleNamespace  # noqa: E402

import bittensor as bt  # noqa: E402

from allways.solana import pdas  # noqa: E402
from allways.validator import weights_vote  # noqa: E402
from allways.validator_v2.actuator import executors  # noqa: E402
from allways.validator_v2.actuator.planners import HygienePlanner  # noqa: E402
from allways.validator_v2.chains.subtensor_view import Head  # noqa: E402
from allways.validator_v2.runtime.bus import Latest  # noqa: E402
from allways.validator_v2.world.snapshot import World  # noqa: E402


class ParityFloorSweep:
    """CollateralFloorSweep's surface over HygienePlanner: each step builds the World from the fake client
    (the World would already hold these accounts) and sends what the planner plans."""

    RETRY_SECS = 300  # the old sweep's retry cadence; the tests advance their clock by it

    def __init__(self, solana_client, read_only=False, clock=None):
        self._client, self._read_only = solana_client, read_only
        self._clock = clock or (lambda: 0.0)

    def step(self, floor, tao_floor=0):
        now = self._clock()
        states = dict(self._client.get_all('MinerState'))
        bonds = {}
        for key, ms in states.items():
            att = self._client.get_bond_attestation(key, 'tao')
            if att is not None:
                bonds[str(pdas.bond_attestation_pda(key, 'tao'))] = att
        config = SimpleNamespace(min_collateral=floor, tao_min_collateral=tao_floor)
        world = World(now, 1, True, config, {'MinerState': states, 'BondAttestation': bonds})
        for intent in HygienePlanner(Latest())._floor_sweep(world, now):
            if self._read_only:
                continue
            try:
                executors.vote_deactivate(self._client, intent)
            except Exception as e:
                bt.logging.warning(f'floor sweep: {e}')


def parity_vote_weights(vali, now):
    """maybe_vote_weights' surface over HygienePlanner's weights rule."""
    client = vali.solana_client
    config = client.get_config()
    head = Latest()
    head.set(Head(vali.block, now, vali.metagraph, now))
    world = World(now, 1, True, config, {}, attribution=weights_vote.build_attribution(client))
    for intent in HygienePlanner(head, me=client.keypair.pubkey())._weights(world, now):
        if vali.solana_swap_loop.read_only:
            continue
        try:
            executors.vote_set_weights(client, intent)
        except Exception as e:
            bt.logging.warning(f'weights vote: {e}')


class LedgerStore:
    """The old ``ValidatorStateStore`` write surface the scoring tests seed through, landing in a v2
    ``Ledger``; it also stands in for ``SolanaEventIndex`` (the ledger is both). Raw SQL the tests run
    against the old tables is translated row by row."""

    LEDGER_SINCE_KEY = 'clearing_ledger_since'

    def __init__(
        self,
        db_path=None,
        *args: Any,
        reservation_ttl_fn=None,
        fill_qualifier=None,
        providers=None,
        quote_reader=None,
        **kwargs: Any,
    ) -> None:
        from allways.validator_v2.scoring.ledger import Ledger
        from allways.validator_v2.store.db import Store

        if isinstance(db_path, LedgerStore):  # SolanaEventIndex(store, ttl_fn, fill_qualifier=...)
            self.__dict__.update(db_path.__dict__)
            self._ttl = args[0] if args else reservation_ttl_fn
            self._qualifier = fill_qualifier
            self._providers, self._quote_reader, self._refusals = providers or {}, quote_reader, {}
            return
        self._ttl = self._qualifier = self._quote_reader = None
        self._providers, self._refusals = {}, {}
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.store = Store(str(db_path))
        self.ledger = Ledger(self.store)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__dict__['ledger'], name)

    def insert_rate_event(self, hotkey, from_chain, to_chain, rate, block, collateral_chain='sol'):
        from allways.validator_v2.scoring.ledger import RATE

        self.ledger.put(block, hotkey, RATE, float(rate), from_chain, to_chain, collateral_chain)

    def insert_active_event(self, block, hotkey, active):
        from allways.validator_v2.scoring.ledger import ACTIVE

        self.ledger.put(block, hotkey, ACTIVE, int(bool(active)))

    def insert_activity_event(self, block, hotkey, transition, hub=None):
        from allways.validator_v2.scoring.ledger import ACTIVITY

        self.ledger.put(block, hotkey, ACTIVITY, int(transition), hub=hub)

    def insert_collateral_event(self, block, hotkey, amount, backing='sol'):
        from allways.validator_v2.scoring.ledger import COLLATERAL

        self.ledger.put(block, hotkey, COLLATERAL, int(amount), backing=backing)

    def insert_clearing_rate(
        self,
        block,
        hotkey,
        from_chain,
        to_chain,
        from_amount,
        to_amount,
        swap_key=None,
        backing='sol',
        qualified=False,
        collateral_amount=0,
    ):
        import uuid

        legs = (from_amount, to_amount, collateral_amount)
        lane = (from_chain, to_chain, backing)
        self.ledger.put_fill(swap_key or uuid.uuid4().hex, block, hotkey, lane, legs, qualified)

    def ingest(self, records, attribution):
        """The old batch ingest: attribute each record, then fold it through the v2 ledger."""
        ttl = int(self._ttl()) if self._ttl else 0
        written = 0
        for rec in records:
            hotkey = attribution.get(str(rec.fields.get('miner')))
            if hotkey is not None and rec.block_time is not None:
                usable = self._judge(rec) if rec.name == 'QuoteSet' else None
                written += self.ledger.ingest(rec, hotkey, ttl, usable, self._qualifier)
        return written

    def _judge(self, rec):
        """v2 judges a QuoteSet from the addresses on the event; the old tests hand them over as the quote's
        account, so read that, then judge with v2's rule. The refusal memo stands in for the chain watcher's."""
        from allways.validator_v2.scoring.component import quote_usable

        f = rec.fields
        if self._quote_reader is None:
            return None
        try:
            quote = self._quote_reader(str(f['miner']), f['from_chain'], f['to_chain'], f['collateral_chain'])
        except Exception:
            return None
        if quote is None:
            return None

        def refused(chain, address):
            provider = self._providers.get(chain)
            if provider is None:
                return False
            key = (chain, provider.chain.normalize_address(address))
            if key not in self._refusals:
                try:
                    self._refusals[key] = bool(provider.address_refused(address))
                except Exception:
                    return False
            return self._refusals[key]

        return quote_usable(quote, self._providers, refused)

    def lanes_with_live_rate(self, at):
        return self.ledger._live_rates(at)

    def get_clearing_volumes(self, start, end):
        out = {}
        rows = self.store.conn().execute('SELECT * FROM scoring_fills WHERE at > ? AND at <= ?', (start, end))
        for r in rows:
            d = out.setdefault((r['from_chain'], r['to_chain']), {})
            a, b = d.get(r['hotkey'], (0, 0))
            d[r['hotkey']] = (a + int(r['from_amount']), b + int(r['to_amount']))
        return out

    def prune_clearing_rates(self, cutoff):
        self.store.conn().execute('DELETE FROM scoring_fills WHERE at < ?', (cutoff,))

    def set_relay_meta(self, key, value):
        from allways.validator_v2.scoring.ledger import LEDGER_SINCE
        from allways.validator_v2.store.db import Store

        with self.store.tx() as c:
            Store.set_meta(c, LEDGER_SINCE if key == self.LEDGER_SINCE_KEY else key, value)

    def record_address_ban(self, hotkey, at):
        self.ledger.ban(hotkey, at)

    def restamp_reservation_expiry(self, hotkey, hub, at, not_before=0):
        self.ledger.restamp_expiry(hotkey, hub, at, not_before)

    def get_latest_rate_before(self, hotkey, from_chain, to_chain, block, collateral_chain=None):
        rates = self.ledger.get_latest_rates_before(from_chain, to_chain, block, collateral_chain)
        if collateral_chain is None:  # the old single-row read takes the latest across backings
            rows = [
                r
                for r in self._all(2)
                if r['hotkey'] == hotkey
                and r['from_chain'] == from_chain
                and r['to_chain'] == to_chain
                and r['at'] <= block
            ]
            return (rows[-1]['value'], rows[-1]['at']) if rows else None
        return rates.get(hotkey)

    def _all(self, kind):
        return (
            self.store.conn().execute('SELECT * FROM scoring_events WHERE kind = ? ORDER BY at, id', (kind,)).fetchall()
        )

    def load_all_active_events(self):
        return [{'block_num': r['at'], 'hotkey': r['hotkey'], 'active': bool(r['value'])} for r in self._all(0)]

    def load_all_activity_events(self):
        return [
            {'block_num': r['at'], 'hotkey': r['hotkey'], 'kind': int(r['value']), 'hub': r['hub']}
            for r in self._all(1)
        ]

    def load_all_collateral_events(self):
        return [{'block_num': r['at'], 'hotkey': r['hotkey'], 'collateral_rao': int(r['value'])} for r in self._all(3)]

    def get_active_state_at(self, at):
        return self.ledger.get_active_miners_at(at)

    def get_collaterals_at(self, at, backing=None):
        return self.ledger.get_miner_collaterals_at(at, backing=backing)

    def require_connection(self):
        return _SqlShim(self)

    def close(self):
        pass


class _SqlShim:
    """The INSERT/DELETE shapes the scoring tests run against the old tables."""

    def __init__(self, store: LedgerStore) -> None:
        self.store = store

    def execute(self, sql: str, params=()):
        import re

        m = re.match(r'\s*INSERT INTO (\w+) \(([^)]*)\) VALUES', sql)
        if m:
            row = dict(zip([c.strip() for c in m.group(2).split(',')], params))
            if m.group(1) == 'rate_events':
                self.store.insert_rate_event(**row)
            elif m.group(1) == 'clearing_rates':
                row['block'] = row.pop('block_num')
                self.store.insert_clearing_rate(**row)
            else:
                raise NotImplementedError(sql)
            return
        m = re.match(r"\s*DELETE FROM active_events WHERE hotkey = '(\w+)'", sql)
        if m:
            from allways.validator_v2.scoring.ledger import ACTIVE

            self.store.store.conn().execute(
                'DELETE FROM scoring_events WHERE kind = ? AND hotkey = ?', (ACTIVE, m.group(1))
            )
            return
        raise NotImplementedError(sql)

    def executemany(self, sql: str, rows):
        for row in rows:
            self.execute(sql, row)

    def commit(self):
        pass


def under_ledger(test_module: str, skip=()) -> dict:
    """A fresh copy of ``tests/<test_module>.py`` whose store and event index are ``LedgerStore``: the test
    names, fixtures and helpers to splice into a parity module, minus ``skip``."""
    import importlib.util

    path = Path(__file__).parent.parent / f'{test_module}.py'
    spec = importlib.util.spec_from_file_location(f'tests.validator_v2._{test_module}_under_ledger', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.ValidatorStateStore = LedgerStore
    mod.SolanaEventIndex = LedgerStore
    return {k: v for k, v in vars(mod).items() if not k.startswith('__') and k not in skip}

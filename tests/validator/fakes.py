"""Test doubles for the serving request ops: a context over a fake program client, and its store."""

import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

from allways.solana.events import EventRecord
from allways.validator.actuator.swap_context import SwapMemory
from allways.validator.serving.store import ServingStore
from allways.validator.store.db import Store


def serving_store(path=None, world=None, providers=None) -> ServingStore:
    store = Store(str(path or Path(tempfile.mkdtemp()) / 'state.db'))
    return ServingStore(store, lambda: world, SwapMemory(store), providers or {})


class Ctx:
    """What the request ops read off the serving component. The fake ``client`` answers the reads (as the
    World would) and records the sends."""

    def __init__(
        self,
        client,
        store=None,
        providers=None,
        metagraph=None,
        states=None,
        report=None,
        subtensor=None,
        vault=None,
        netuid=19,
    ):
        self.client = client
        self.store = store if store is not None else serving_store()
        self.providers = providers if providers is not None else {}
        self.metagraph = metagraph or SimpleNamespace(hotkeys=[], S=[], n=SimpleNamespace(item=lambda: 0))
        self.states, self._report = states or {}, report or {}
        self.subtensor, self._vault, self.netuid = subtensor, vault, netuid
        self.lock = threading.RLock()

    def accounts(self):
        return self.client

    def miner_states(self):
        return self.states

    def report(self):
        return self._report

    def vault(self):
        return self._vault


def record_outcome(store, swap_key_hex, outcome, at, refund=None):
    """The terminal event that would close the swap, as the store ingests it."""
    name = {'completed': 'SwapCompleted', 'timed_out': 'SwapTimedOut', 'cancelled': 'SwapCancelled'}[outcome]
    fields = {'swap_key': bytes.fromhex(swap_key_hex)}
    signature = 's'
    if refund is not None:
        chain, amount, signature = refund
        fields.update(collateral_chain=chain, reimbursement=amount)
    elif name == 'SwapTimedOut':
        fields.update(collateral_chain='sol', reimbursement=0)
    store.ingest(EventRecord(name, fields, 1, at, signature))
    if name == 'SwapTimedOut' and refund is None:  # a verdict from before refunds were reported
        store.store.conn().execute(
            'UPDATE serving_outcomes SET refund_chain = NULL, refund_amount = NULL, refund_tx = NULL WHERE swap_key = ?',
            (swap_key_hex,),
        )


def record_fulfillment(store, swap_key_hex, to_tx_hash, at):
    store.ingest(
        EventRecord('SwapFulfilled', {'swap_key': bytes.fromhex(swap_key_hex), 'to_tx_hash': to_tx_hash}, 1, at, 's')
    )

"""A tx whose logs the runtime truncated can change accounts with no event saying so: the feed names every
account it touched, and the World re-reads them, typed by owner and discriminator."""

import base64

from solders.keypair import Keypair
from solders.pubkey import Pubkey

from allways.solana import layouts, pdas
from allways.solana.events import LOG_TRUNCATED, TX_LOGS_TRUNCATED, EventRecord
from allways.validator.feed.events import TRUNCATED_REREADS_PER_HOUR, HistoryGap, LiveFeed, SlotClock, _Pending
from tests.validator.test_feed import NOW, _FakeLive, _feed, _SigRpc
from tests.validator.test_world import MINER, _Client, _quote_fields, _reader

PROGRAM = str(Keypair().pubkey())
SIGNER, OTHER = str(Keypair().pubkey()), str(Keypair().pubkey())


class _TxRpc(_SigRpc):
    def __init__(self, keys, history=()):
        super().__init__(list(history))
        self.keys = keys

    def get_transaction(self, sig):
        self.tx_reads.append(sig)
        if self.keys is None:
            return None
        return {'blockTime': NOW, 'transaction': {'message': {'accountKeys': self.keys}}, 'meta': {'logMessages': []}}


def _live_feed(tmp_path, rpc):
    live = _FakeLive()
    feed, events, gaps = _feed(tmp_path, rpc, live)
    feed.setup()
    feed._clock = SlotClock(lambda slot: NOW, min_interval=0)
    feed.tick(NOW)  # finishes the (empty) boot backfill
    return feed, live, events, gaps


def test_the_live_socket_flags_a_truncated_tx():
    feed = LiveFeed('ws://x', PROGRAM)
    logs = [f'Program {PROGRAM} invoke [1]', LOG_TRUNCATED]
    feed.handle_notification(
        {'params': {'result': {'context': {'slot': 7}, 'value': {'signature': 's', 'err': None, 'logs': logs}}}}
    )
    (pending,) = feed.take()
    assert pending.truncated


def test_a_truncated_live_tx_names_every_account_it_touched(tmp_path):
    rpc = _TxRpc([SIGNER, OTHER])
    feed, live, events, _ = _live_feed(tmp_path, rpc)
    live.inbox.append(_Pending(9, 'live-1', [], truncated=True))
    feed.tick(NOW)
    (record,) = events.drain()
    assert (record.name, record.fields['accounts']) == (TX_LOGS_TRUNCATED, [SIGNER, OTHER])
    assert rpc.tx_reads == ['live-1']


def test_an_untruncated_live_tx_needs_no_transaction_read(tmp_path):
    rpc = _TxRpc([SIGNER])
    feed, live, events, _ = _live_feed(tmp_path, rpc)
    live.inbox.append(_Pending(9, 'live-1', [('QuoteRemoved', {})]))
    feed.tick(NOW)
    assert [r.name for r in events.drain()] == ['QuoteRemoved'] and rpc.tx_reads == []


def test_an_unreadable_truncated_tx_falls_back_to_a_rescan(tmp_path):
    feed, live, events, gaps = _live_feed(tmp_path, _TxRpc(None))
    live.inbox.append(_Pending(9, 'live-1', [], truncated=True))
    feed.tick(NOW)
    assert events.drain() == [] and gaps.drain()[-1] == HistoryGap(NOW)  # after the boot backfill's own gap


def test_past_the_hourly_cap_truncated_txs_are_left_to_the_rescan(tmp_path):
    rpc = _TxRpc([SIGNER])
    feed, live, events, _ = _live_feed(tmp_path, rpc)
    for i in range(TRUNCATED_REREADS_PER_HOUR + 5):
        live.inbox.append(_Pending(9 + i, f'live-{i}', [], truncated=True))
    feed.tick(NOW)
    assert len(events.drain()) == TRUNCATED_REREADS_PER_HOUR
    assert len(rpc.tx_reads) == TRUNCATED_REREADS_PER_HOUR


def test_a_truncated_backfilled_tx_is_named_from_the_tx_already_read(tmp_path):
    class Rpc(_SigRpc):
        def get_transaction(self, sig):
            self.tx_reads.append(sig)
            return {
                'blockTime': NOW,
                'transaction': {'message': {'accountKeys': [SIGNER]}},
                'meta': {'logMessages': [LOG_TRUNCATED], 'loadedAddresses': {'writable': [OTHER], 'readonly': []}},
            }

    rpc = Rpc([{'signature': f's-{NOW}', 'slot': 9, 'blockTime': NOW, 'err': None}])
    feed, events, _ = _feed(tmp_path, rpc, _FakeLive(), resume='before-history')
    feed.setup()
    feed._backfill.until_sig = None
    for _ in range(3):
        feed.tick(NOW)
    (record,) = events.drain()
    assert record.name == TX_LOGS_TRUNCATED and record.fields['accounts'] == [SIGNER, OTHER]
    assert rpc.tx_reads == [f's-{NOW}']  # no second read for the accounts


def _owned(client, pda, raw, owner):
    client.live[str(pda)] = {'data': [base64.b64encode(raw).decode(), 'base64'], 'owner': owner}


def test_the_world_rereads_a_truncated_txs_accounts_and_types_them_itself(tmp_path):
    client = _Client()
    client.program_id = Pubkey.from_string(PROGRAM)
    quote_pda = str(pdas.quote_pda(MINER, 'btc', 'sol', 'sol', client.program_id))
    reader, events, _, _, out = _reader(client, tmp_path)
    reader.tick(NOW)  # boot scan
    events.publish(EventRecord('QuoteSet', _quote_fields(rate=5), 200, NOW, 'q'))
    reader.tick(NOW)
    # The miner then worsened its rate in a tx whose QuoteSet event the runtime cut off.
    quote = {k: v for k, v in _quote_fields(rate=9).items() if k != 'update_fee'}
    raw = layouts.DISCRIMINATORS['MinerQuote'] + layouts.ACCOUNT_LAYOUTS['MinerQuote'].build(
        dict(quote, miner=bytes(MINER), bump=0)
    )
    _owned(client, quote_pda, raw, PROGRAM)
    foreign = str(Keypair().pubkey())
    _owned(client, foreign, raw, OTHER)  # same bytes, another program's account: not ours
    held = str(pdas.swap_pda(bytes(range(32)), client.program_id))
    reader._accounts['Swap'][held] = 'stale'  # closed in that tx: reads back null
    client.slot = 300
    events.publish(EventRecord(TX_LOGS_TRUNCATED, {'accounts': [SIGNER, quote_pda, foreign, held]}, 300, NOW, 't'))
    reader.tick(NOW)
    world, _ = out.get()
    assert world.get('MinerQuote', quote_pda).rate == 9
    assert world.get('MinerQuote', foreign) is None
    assert world.get('Swap', held) is None

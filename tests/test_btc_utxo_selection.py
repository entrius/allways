"""BTC payout coin selection skips unconfirmed outside coins.

The miner's payout address is public, so anyone can send it a large unconfirmed coin. A payout
built on that coin dies when the sender RBFs it away (or never confirms it), and the swap times out.
"""

from embit.script import address_to_scriptpubkey

DEST = address_to_scriptpubkey('bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4')
CHANGE = address_to_scriptpubkey('bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4')


def _provider(monkeypatch):
    monkeypatch.setenv('BTC_MODE', 'lightweight')
    from allways.assets.btc import Bitcoin

    return Bitcoin()


def _utxo(txid_byte: str, value: int, confirmed: bool) -> dict:
    return {'txid': txid_byte * 32, 'vout': 0, 'value': value, 'status': {'confirmed': confirmed}}


def _select(p, utxos, amount):
    return p.select_utxos(utxos, amount, True, [DEST, CHANGE], fee_rate_override=1)


def test_large_unconfirmed_outside_coin_is_skipped(monkeypatch):
    p = _provider(monkeypatch)
    planted = _utxo('ee', 5_000_000, confirmed=False)
    own = _utxo('aa', 200_000, confirmed=True)
    selected, total_in, _ = _select(p, [planted, own], 100_000)
    assert selected == [own]
    assert total_in == 200_000


def test_only_unconfirmed_outside_coins_cannot_fund(monkeypatch):
    p = _provider(monkeypatch)
    assert _select(p, [_utxo('ee', 5_000_000, confirmed=False)], 100_000) is None
    assert 'awaiting confirmation' in p.last_send_error


def test_own_unconfirmed_change_is_spendable(monkeypatch):
    p = _provider(monkeypatch)
    change = _utxo('bb', 300_000, confirmed=False)
    p.broadcasted_txids['bb' * 32] = 'swap-1'
    selected, _, _ = _select(p, [change], 100_000)
    assert selected == [change]


def test_confirmed_coin_preferred_over_larger_own_change(monkeypatch):
    """Own unconfirmed change sits in a mempool chain the payee of that tx can grow; spend confirmed first."""
    p = _provider(monkeypatch)
    change = _utxo('bb', 300_000, confirmed=False)
    p.broadcasted_txids['bb' * 32] = 'swap-1'
    confirmed = _utxo('aa', 150_000, confirmed=True)
    selected, _, _ = _select(p, [change, confirmed], 100_000)
    assert selected == [confirmed]


def test_own_change_tops_up_when_confirmed_is_short(monkeypatch):
    p = _provider(monkeypatch)
    change = _utxo('bb', 300_000, confirmed=False)
    p.broadcasted_txids['bb' * 32] = 'swap-1'
    confirmed = _utxo('aa', 50_000, confirmed=True)
    selected, _, _ = _select(p, [change, confirmed], 100_000)
    assert selected == [confirmed, change]


# --- broadcast refused for a mempool chain limit -> rebuild without that change ---

TEST_WIF = 'L1RrrnXkcKut5DEMwtDthjwRcTTwED36thyL1DebVrKuwvohjMNi'
PAYEE = 'bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4'
CHAIN_LIMIT_BODY = (
    'sendrawtransaction RPC error: {"code":-26,"message":"too-long-mempool-chain, '
    'too many descendants for tx ' + 'bb' * 32 + ' [limit: 25]"}'
)


class _Resp:
    def __init__(self, status_code: int, text: str = '', body=None):
        self.status_code = status_code
        self.text = text
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        pass


def _send_setup(monkeypatch, utxos, post_bodies):
    from embit.ec import PrivateKey
    from embit.networks import NETWORKS
    from embit.script import p2wpkh

    monkeypatch.setenv('BTC_NETWORK', 'mainnet')
    monkeypatch.setenv('BTC_PRIVATE_KEY', TEST_WIF)
    p = _provider(monkeypatch)
    sender = p2wpkh(PrivateKey.from_wif(TEST_WIF).get_public_key()).address(NETWORKS['main'])
    monkeypatch.setattr(p, 'btc_api_get', lambda path, timeout=15: _Resp(200, body=[dict(u) for u in utxos]))
    monkeypatch.setattr(p, 'find_recent_outgoing', lambda *a: None)
    monkeypatch.setattr(p, 'tx_exists', lambda txid: False)
    posted = []

    def post(path, data, timeout=30):
        posted.append(data)
        status, text = post_bodies[len(posted) - 1]
        return _Resp(status, text)

    monkeypatch.setattr(p, 'btc_api_post', post)
    return p, sender, posted


def _spent_txids(raw_hex: str) -> set:
    from embit.transaction import Transaction

    return {vin.txid.hex() for vin in Transaction.from_string(raw_hex).vin}


def test_chain_limit_rejection_rebuilds_without_that_change(monkeypatch):
    pinned = _utxo('bb', 300_000, confirmed=False)
    other = _utxo('cc', 250_000, confirmed=False)
    p, sender, posted = _send_setup(monkeypatch, [pinned, other], [(400, CHAIN_LIMIT_BODY), (200, 'f' * 64)])
    p.broadcasted_txids['bb' * 32] = 'swap-A'
    p.broadcasted_txids['cc' * 32] = 'swap-C'

    result = p.send_amount_lightweight(PAYEE, 100_000, from_address=sender, fee_rate_override=1, dedup_key='swap-B')

    assert result == ('f' * 64, 0)
    assert len(posted) == 2
    assert _spent_txids(posted[0]) == {'bb' * 32}
    assert _spent_txids(posted[1]) == {'cc' * 32}
    # Remembered: the next payout doesn't walk into the same refusal.
    assert not p.is_spendable_utxo(pinned)
    assert p.is_spendable_utxo({**pinned, 'status': {'confirmed': True}})


def test_chain_limit_rejection_falls_back_to_confirmed_top_up(monkeypatch):
    """Confirmed alone is short, so change tops it up; refused → the next own change replaces it."""
    confirmed = _utxo('aa', 50_000, confirmed=True)
    pinned = _utxo('bb', 300_000, confirmed=False)
    other = _utxo('cc', 250_000, confirmed=False)
    p, sender, posted = _send_setup(monkeypatch, [pinned, other, confirmed], [(400, CHAIN_LIMIT_BODY), (200, 'f' * 64)])
    p.broadcasted_txids['bb' * 32] = 'swap-A'
    p.broadcasted_txids['cc' * 32] = 'swap-C'

    assert p.send_amount_lightweight(PAYEE, 100_000, from_address=sender, fee_rate_override=1) == ('f' * 64, 0)
    assert _spent_txids(posted[0]) == {'aa' * 32, 'bb' * 32}
    assert _spent_txids(posted[1]) == {'aa' * 32, 'cc' * 32}


def test_chain_limit_with_no_alternative_fails_without_looping(monkeypatch):
    pinned = _utxo('bb', 300_000, confirmed=False)
    p, sender, posted = _send_setup(monkeypatch, [pinned], [(400, CHAIN_LIMIT_BODY)])
    p.broadcasted_txids['bb' * 32] = 'swap-A'

    assert p.send_amount_lightweight(PAYEE, 100_000, from_address=sender, fee_rate_override=1) is None
    assert len(posted) == 1
    assert 'awaiting confirmation' in p.last_send_error


def test_other_rejection_does_not_exclude_change(monkeypatch):
    pinned = _utxo('bb', 300_000, confirmed=False)
    p, sender, posted = _send_setup(monkeypatch, [pinned], [(400, 'min relay fee not met')])
    p.broadcasted_txids['bb' * 32] = 'swap-A'

    assert p.send_amount_lightweight(PAYEE, 100_000, from_address=sender, fee_rate_override=1) is None
    assert len(posted) == 1
    assert p.is_spendable_utxo(pinned)


def test_chain_limit_markers_cover_node_reject_reasons():
    from allways.assets.btc import is_mempool_chain_limit_error

    for text in (
        CHAIN_LIMIT_BODY,
        'too-long-mempool-chain, too many unconfirmed ancestors [limit: 25]',
        'too-long-mempool-chain, exceeds descendant size limit for tx x [limit: 101000]',
        'too-large-cluster',
    ):
        assert is_mempool_chain_limit_error(text)
    assert not is_mempool_chain_limit_error('bad-txns-inputs-missingorspent')
    assert not is_mempool_chain_limit_error(None)

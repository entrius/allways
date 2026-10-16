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

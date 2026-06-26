"""Unit tests for the SOL-numéraire quote derivation (one price per chain → all directions)."""

from allways.cli.swap_commands.numeraire import derive_sol_numeraire_quotes


def test_derives_both_directions_per_chain():
    specs = derive_sol_numeraire_quotes('SOLADDR', {'btc': (0.002, 'BTCADDR'), 'tao': (0.5, 'TAOADDR')})
    pairs = {(s.from_chain, s.to_chain): s for s in specs}
    assert set(pairs) == {('sol', 'btc'), ('btc', 'sol'), ('sol', 'tao'), ('tao', 'sol')}
    # zero spread ⇒ both directions store the same canonical 'X per SOL' rate.
    assert pairs[('sol', 'btc')].rate == 0.002
    assert pairs[('btc', 'sol')].rate == 0.002


def test_addresses_oriented_per_direction():
    specs = derive_sol_numeraire_quotes('SOLADDR', {'btc': (0.002, 'BTCADDR')})
    fwd = next(s for s in specs if s.from_chain == 'sol')
    rev = next(s for s in specs if s.to_chain == 'sol')
    assert (fwd.from_addr, fwd.to_addr) == ('SOLADDR', 'BTCADDR')
    assert (rev.from_addr, rev.to_addr) == ('BTCADDR', 'SOLADDR')


def test_spread_applies_symmetric_margin():
    # 100 bps: sol->X at price*0.99, X->sol at price*1.01.
    specs = derive_sol_numeraire_quotes('S', {'btc': (1.0, 'B')}, spread_bps=100)
    fwd = next(s for s in specs if s.from_chain == 'sol')
    rev = next(s for s in specs if s.to_chain == 'sol')
    assert fwd.rate == 0.99
    assert rev.rate == 1.01


def test_skips_sol_and_nonpositive_prices():
    specs = derive_sol_numeraire_quotes('S', {'sol': (1.0, 'S'), 'btc': (0.0, 'B'), 'tao': (-1.0, 'T')})
    assert specs == []

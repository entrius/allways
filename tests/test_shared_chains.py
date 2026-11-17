"""One chain object per network: every asset on a network shares its tip, session and caches, and keeps its own
send records."""

from types import SimpleNamespace

import pytest

from allways import assets as cp
from allways.assets.tao import TaoChain
from allways.chains import SUPPORTED_CHAINS
from allways.validator.app import chain_watchers
from allways.validator.chain.gateway import Meter


@pytest.fixture
def providers(monkeypatch):
    for chain_def in SUPPORTED_CHAINS.values():
        if chain_def.host_chain not in (None, 'solana'):
            monkeypatch.setenv(f'{chain_def.env_prefix}_NETWORK', 'mainnet')
            monkeypatch.delenv(f'{chain_def.env_prefix}_RPC_URLS', raising=False)
    return cp.create_assets(subtensor=SimpleNamespace(), solana_rpc_url='http://127.0.0.1:8899')


def test_every_asset_on_a_network_shares_one_chain_object(providers):
    assert providers['eth'].chain is providers['ethusdc'].chain is providers['uni'].chain
    assert providers['arbusdc'].chain is not providers['eth'].chain  # another network, another object
    assert providers['sol'].chain is providers['solusdc'].chain
    alphas = [p for chain_id, p in providers.items() if chain_id.startswith('sn')]
    assert alphas and all(p.chain is providers['tao'].chain for p in alphas)
    assert isinstance(providers['tao'].chain, TaoChain)


def test_a_shared_chain_gets_one_watcher_and_one_metered_session(providers):
    watchers = chain_watchers(providers)
    assert watchers['sol'] is watchers['solusdc'] and watchers['tao'] is watchers['sn7']
    assert watchers['eth'] is watchers['ethusdc'] and watchers['eth'] is not watchers['arbusdc']
    sessions = {id(s) for p in providers.values() for s in p.chain.http_sessions()}
    assert len(sessions) == len({id(p.chain) for p in providers.values()}) - 1  # the subtensor has none
    Meter([]).attach_chains(providers.values())


def test_sol_and_an_spl_token_on_one_chain_never_reuse_each_others_payout(providers):
    """Each asset keeps its own broadcast records, so a SOL payout can't stand in for a USDC one."""
    sol, usdc = providers['sol'], providers['solusdc']
    want = ('RECIP', 5_000_000, '')
    sol.broadcasted_txids['SOLSIG'] = (*want, 1000)
    assert usdc.broadcasted_txids == {}
    assert usdc.chain.prior_broadcast(usdc.broadcasted_txids, want) is None


def test_a_pass_empties_the_block_caches_and_the_bound_holds_between_passes():
    chain = TaoChain(SimpleNamespace())
    chain.block_cache[1], chain.block_hash_cache[1], chain.events_cache['0x1'] = {}, '0x1', []
    chain._pass_tip = 100
    chain.start_pass()
    assert chain.block_cache == chain.block_hash_cache == chain.events_cache == {} and chain._pass_tip is None
    for n in range(TaoChain._CACHE_MAX + 10):
        TaoChain._remember(chain.block_cache, n, {'n': n})
    assert len(chain.block_cache) == TaoChain._CACHE_MAX and min(chain.block_cache) == 10

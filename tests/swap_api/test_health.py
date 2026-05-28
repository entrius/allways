from tests.swap_api.conftest import FakeContractClient, FakeSubtensor


def test_healthz_returns_block_and_contract(client_factory):
    client = client_factory(contract_client=FakeContractClient(), subtensor=FakeSubtensor(block=12345))
    resp = client.get('/healthz')
    assert resp.status_code == 200
    body = resp.json()
    assert body['ok'] is True
    assert body['chainBlock'] == 12345
    assert body['contractAddress'].startswith('5')


def test_chains_lists_supported_chains(client_factory):
    client = client_factory(contract_client=FakeContractClient(), subtensor=FakeSubtensor())
    resp = client.get('/chains')
    assert resp.status_code == 200
    body = resp.json()
    chain_ids = {c['id'] for c in body['chains']}
    assert {'btc', 'tao'}.issubset(chain_ids)
    # Pairs are every ordered (a, b) with a != b.
    assert ['btc', 'tao'] in [list(p) for p in body['pairs']]
    assert ['tao', 'btc'] in [list(p) for p in body['pairs']]


def test_proofs_match_canonical_format(client_factory):
    client = client_factory(contract_client=FakeContractClient(), subtensor=FakeSubtensor())
    r1 = client.get('/proofs/reserve', params={'address': 'bc1qx', 'block': 42})
    assert r1.json() == {'message': 'allways-reserve:bc1qx:42'}
    r2 = client.get('/proofs/confirm', params={'txHash': 'deadbeef'})
    assert r2.json() == {'message': 'allways-swap:deadbeef'}

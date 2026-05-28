from tests.swap_api.conftest import FakeContractClient, FakeSubtensor, make_pair


def test_best_miner_picks_highest_rate_for_canonical_forward(client_factory):
    cheap = make_pair('5Ccheap', rate=300.0, uid=1)
    rich = make_pair('5Crich', rate=400.0, uid=2)
    contract = FakeContractClient()
    contract.add_miner('5Ccheap', collateral=1_000_000_000)
    contract.add_miner('5Crich', collateral=1_000_000_000)
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(block=500),
        miner_pairs=[cheap, rich],
    )

    resp = client.get('/miners/best', params={'from': 'btc', 'to': 'tao', 'amount': 10_000})
    assert resp.status_code == 200
    body = resp.json()
    assert body['minerHotkey'] == '5Crich'
    assert body['rate'] == '400'
    assert body['freshAsOf'] == 500


def test_best_miner_skips_ineligible(client_factory):
    busy = make_pair('5Cbusy', rate=400.0)
    healthy = make_pair('5Chealthy', rate=300.0)
    contract = FakeContractClient()
    contract.add_miner('5Cbusy', has_swap=True, collateral=1_000_000_000)
    contract.add_miner('5Chealthy', collateral=1_000_000_000)
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        miner_pairs=[busy, healthy],
    )

    resp = client.get('/miners/best', params={'from': 'btc', 'to': 'tao', 'amount': 1})
    assert resp.status_code == 200
    assert resp.json()['minerHotkey'] == '5Chealthy'


def test_best_miner_404_when_no_eligible(client_factory):
    contract = FakeContractClient()
    contract.add_miner('5Csolo', active=False, collateral=1)
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        miner_pairs=[make_pair('5Csolo')],
    )
    resp = client.get('/miners/best', params={'from': 'btc', 'to': 'tao', 'amount': 1})
    assert resp.status_code == 404


def test_miners_list_returns_summaries(client_factory):
    contract = FakeContractClient()
    contract.add_miner('5Ca', collateral=5_000)
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        miner_pairs=[make_pair('5Ca', rate=345.0)],
    )
    resp = client.get('/miners', params={'from': 'btc', 'to': 'tao'})
    assert resp.status_code == 200
    body = resp.json()
    assert body[0]['hotkey'] == '5Ca'
    assert body[0]['rate'] == '345'
    assert body[0]['collateralRao'] == 5_000


def test_best_miner_rejects_same_from_to(client_factory):
    client = client_factory(contract_client=FakeContractClient(), subtensor=FakeSubtensor())
    resp = client.get('/miners/best', params={'from': 'btc', 'to': 'btc', 'amount': 1})
    assert resp.status_code == 400

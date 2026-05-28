from types import SimpleNamespace

from tests.swap_api.conftest import (
    FakeContractClient,
    FakeSubtensor,
    make_reservation,
    make_swap,
)

RESERVE_BODY = {
    'minerHotkey': '5Cminer',
    'fromChain': 'btc',
    'toChain': 'tao',
    'taoAmount': 3_450_000,
    'fromAmount': 10_000,
    'toAmount': 3_450_000,
    'fromAddress': 'bc1qsource',
    'fromAddressProof': '0xsig',
    'blockAnchor': 100,
    'expectedRate': '345',
}


def _accepted_response():
    return SimpleNamespace(accepted=True, rejection_reason='')


def _rejected_response(reason: str):
    return SimpleNamespace(accepted=False, rejection_reason=reason)


def test_reserve_returns_409_on_rate_drift(client_factory):
    contract = FakeContractClient()
    contract.add_miner('5Cminer', reserved_until=200)
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        commitments={'5Cminer': 'v1:btc:bc1qsource:tao:5Cdest:999:888'},
        broadcast_factory=lambda _s: [_accepted_response()],
    )
    resp = client.post('/reserve', json=RESERVE_BODY)
    assert resp.status_code == 409
    body = resp.json()
    assert body['code'] == 'RateChanged'
    assert body['expected'] == '345'
    assert body['actual'] == '999'


def test_reserve_succeeds_when_rate_matches(client_factory):
    contract = FakeContractClient()
    contract.add_miner('5Cminer', reserved_until=250)
    contract.reservations['5Cminer'] = make_reservation('5Cminer', request_hash='0xfeed', reserved_until=250)
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        commitments={'5Cminer': 'v1:btc:bc1qmsrc:tao:5Cmdest:345:0.003'},
        broadcast_factory=lambda _s: [_accepted_response(), _accepted_response()],
    )

    resp = client.post('/reserve', json=RESERVE_BODY)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body['requestHash'] == '0xfeed'
    assert body['reservedUntilBlock'] == 250
    assert body['minerHotkey'] == '5Cminer'
    assert body['minerSourceAddress'] == 'bc1qmsrc'


def test_reserve_502_when_all_rejected(client_factory):
    contract = FakeContractClient()
    contract.add_miner('5Cminer', reserved_until=0)
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        commitments={'5Cminer': 'v1:btc:bc1qsource:tao:5Cdest:345:0.003'},
        broadcast_factory=lambda _s: [_rejected_response('insufficient source balance')],
    )
    resp = client.post('/reserve', json=RESERVE_BODY)
    assert resp.status_code == 502


def test_reserve_504_when_quorum_does_not_land(client_factory):
    contract = FakeContractClient()
    contract.add_miner('5Cminer', reserved_until=0)
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        commitments={'5Cminer': 'v1:btc:bc1qsource:tao:5Cdest:345:0.003'},
        broadcast_factory=lambda _s: [_accepted_response()],
    )
    resp = client.post('/reserve', json=RESERVE_BODY)
    assert resp.status_code == 504


def test_confirm_returns_swap_id_when_active_swap_exists(client_factory):
    contract = FakeContractClient()
    contract.add_miner('5Cminer', has_swap=True)
    contract.miner_swaps['5Cminer'] = [make_swap(42, '5Cminer')]
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        broadcast_factory=lambda _s: [_accepted_response()],
    )
    body = {
        'requestHash': '0xfeed',
        'minerHotkey': '5Cminer',
        'fromTxHash': 'abc123',
        'fromTxProof': '0xsig',
        'fromAddress': 'bc1qsource',
        'toAddress': '5Cdest',
        'fromChain': 'btc',
        'toChain': 'tao',
        'fromTxBlock': 999,
    }
    resp = client.post('/confirm', json=body)
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload['accepted'] is True
    assert payload['swapId'] == 42


def test_reserve_passes_contract_client_to_validator_discovery(client_factory, monkeypatch):
    """Broadcasts must be filtered to the contract whitelist (spec §4)."""
    contract = FakeContractClient()
    contract.add_miner('5Cminer', reserved_until=250)
    contract.reservations['5Cminer'] = make_reservation('5Cminer', reserved_until=250)

    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        commitments={'5Cminer': 'v1:btc:bc1qmsrc:tao:5Cmdest:345:0.003'},
        broadcast_factory=lambda _s: [_accepted_response()],
    )

    captured: dict = {}

    def fake_discover(subtensor, netuid, contract_client=None):
        captured['contract_client'] = contract_client
        return [object()]

    import allways.swap_api.routes.swap as swap_mod

    # Override the conftest stub so the real wiring through state.contract_client is exercised.
    monkeypatch.setattr(swap_mod, 'discover_validators', fake_discover)
    monkeypatch.setattr(swap_mod, '_discover', lambda s: fake_discover(s.subtensor, s.netuid, s.contract_client))

    client.post('/reserve', json=RESERVE_BODY)
    assert captured.get('contract_client') is contract


def test_confirm_returns_rejection_when_all_validators_reject(client_factory):
    contract = FakeContractClient()
    contract.add_miner('5Cminer')
    client = client_factory(
        contract_client=contract,
        subtensor=FakeSubtensor(),
        broadcast_factory=lambda _s: [_rejected_response('source tx not found')],
    )
    resp = client.post(
        '/confirm',
        json={
            'requestHash': '0xfeed',
            'minerHotkey': '5Cminer',
            'fromTxHash': 'abc123',
            'fromTxProof': '0xsig',
            'fromAddress': 'bc1qsource',
            'toAddress': '5Cdest',
            'fromChain': 'btc',
            'toChain': 'tao',
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body['accepted'] is False
    assert body['swapId'] is None
    assert body['rejection']

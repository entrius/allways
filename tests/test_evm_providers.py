"""Every EVM asset in the registry, offline (RPC layer mocked, signing is pure crypto).

The behaviour is two shared providers — `EvmCoin` for native coins, `Erc20` for tokens — bound to
a registry row each. The deep behaviour of each provider is pinned once, on one representative
(tests/test_ethereum_provider.py, tests/test_arbusdc_provider.py). This module runs the
invariants that a wrong row value would break across every row, then pins the few behaviours
that are genuinely one chain's or one token's own.
"""

import pytest
from eth_account import Account
from eth_utils import keccak

from allways.assets import ASSET_REGISTRY, MissingTestnetDeployment
from allways.assets import evm_coin as evm_coin_module
from allways.assets.asset import ProviderUnreachableError
from allways.assets.avax import Avax
from allways.assets.bnb import Bnb
from allways.assets.cro import Cro
from allways.assets.erc20 import SEL_TRANSFER, TESTNET_TOKEN_CONTRACTS, TRANSFER_TOPIC0, Erc20
from allways.assets.evm import FALLBACK_PRIORITY_FEE_WEI, POLYGON, EvmRpcError
from allways.assets.evm_coin import MAX_WALK_BLOCKS, EvmCoin
from allways.assets.hype import Hype
from allways.assets.pol import Pol
from allways.chains import CHAIN_PAXG, SUPPORTED_CHAINS, get_chain_def
from allways.utils.rate import is_executable_rate
from allways.validator.chain.gateway import Meter, MeteredAdapter

TEST_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'  # hardhat account #0
TEST_ADDR = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'  # the key's address, EIP-55 form
RECIPIENT = '0x70997970C51812dc3A010C7d01b50e0d17dc79C8'  # mixed case on purpose
# fetch_matching_tx takes the canonical (lowercase) form; verify_transaction canonicalizes first.
WIRE_RECIPIENT = RECIPIENT.lower()
OTHER = '0x' + '22' * 20
COUNTERFEIT = '0x' + '99' * 20
TX = '0x' + 'ab' * 32
BLOCK_HASH = '0x' + 'cd' * 32
MINED_BLOCK = 1_000_000

NATIVE_SPECS = tuple(s for s in ASSET_REGISTRY if issubclass(s.asset_cls, EvmCoin))
TOKEN_SPECS = tuple(s for s in ASSET_REGISTRY if issubclass(s.asset_cls, Erc20))
EVM_SPECS = NATIVE_SPECS + TOKEN_SPECS
NO_CHECK_TOKENS = tuple(s for s in TOKEN_SPECS if get_chain_def(s.chain_id).refusal_checks == ())
CHECKED_TOKENS = tuple(s for s in TOKEN_SPECS if get_chain_def(s.chain_id).refusal_checks)


def by_id(spec):
    return spec.chain_id


# A bridged twin beside the native deployment, symbol-identical: only the pinned address tells
# them apart, so it is the impostor the pin is tested against on that chain.
BRIDGED_TWINS = {
    'baseusdc': '0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA',  # USDbC
    'polusdc': '0x2791Bca1f2de4661eD88A30C99A7a9449Aa84174',  # USDC.e
}
CIRCLE_NATIVE = {
    'baseusdc': '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
    'polusdc': '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359',
}

# Same contract, same arity, same indexed slots as Transfer — only topic0 separates them.
APPROVAL_TOPIC0 = '0x' + keccak(text='Approval(address,address,uint256)').hex()
DELEGATE_VOTES_TOPIC0 = '0x' + keccak(text='DelegateVotesChanged(address,uint256,uint256)').hex()


@pytest.fixture(autouse=True)
def mainnet_env(monkeypatch):
    for spec in EVM_SPECS:
        prefix = get_chain_def(spec.chain_id).env_prefix
        monkeypatch.setenv(f'{prefix}_NETWORK', 'mainnet')
        monkeypatch.delenv(f'{prefix}_RPC_URLS', raising=False)
        monkeypatch.delenv(f'{prefix}_PRIVATE_KEY', raising=False)
        monkeypatch.delenv(f'{spec.chain_id.upper()}_TOKEN_CONTRACT', raising=False)


def rpc_stub(provider, responses: dict):
    """Replace eth_rpc with a method→response map. A callable value is invoked with params."""

    def fake_rpc(method, params, timeout=15, **kw):
        value = responses[method]
        return value(params) if callable(value) else value

    provider.chain.eth_rpc = fake_rpc
    return provider


def topic(address: str) -> str:
    return '0x' + '00' * 12 + address.lower().removeprefix('0x')


def amount_for(provider) -> int:
    return 150 * 10**provider.chain_def.decimals


def transfer_log(provider, contract=None, sender=TEST_ADDR, recipient=RECIPIENT, amount=None, topic0=TRANSFER_TOPIC0):
    return {
        'address': contract or provider.token_contract,
        'topics': [topic0, topic(sender), topic(recipient)],
        'data': hex(amount_for(provider) if amount is None else amount),
        'transactionHash': TX,
    }


def mined(provider, status='0x1', confs=None, amount=None, recipient=RECIPIENT, logs=None):
    """A mined leg paying ``amount`` to ``recipient``, ``confs`` deep (default: exactly the row's depth).
    Addresses go on the wire lowercase against checksummed expectations: EIP-55 is display-only."""
    confs = provider.chain_def.min_confirmations if confs is None else confs
    amount = amount_for(provider) if amount is None else amount
    tx = {'hash': TX, 'from': TEST_ADDR.lower(), 'blockNumber': hex(MINED_BLOCK), 'blockHash': BLOCK_HASH}
    receipt = {'status': status, 'blockNumber': hex(MINED_BLOCK), 'blockHash': BLOCK_HASH}
    if isinstance(provider, Erc20):
        tx['to'] = provider.token_contract
        receipt['logs'] = [transfer_log(provider, recipient=recipient, amount=amount)] if logs is None else logs
    else:
        tx.update(to=recipient.lower(), value=hex(amount))
    return {
        'eth_getTransactionByHash': tx,
        'eth_getTransactionReceipt': receipt,
        'eth_blockNumber': hex(MINED_BLOCK + confs - 1),
        'eth_getBlockByNumber': {'hash': BLOCK_HASH, 'timestamp': '0x64'},
    }


def signed_raw(chain_id, gas, max_fee, tip, value=10**16):
    signed = Account.sign_transaction(
        {
            'chainId': chain_id,
            'nonce': 0,
            'to': RECIPIENT,
            'value': value,
            'gas': gas,
            'maxFeePerGas': max_fee,
            'maxPriorityFeePerGas': tip,
        },
        TEST_KEY,
    )
    raw = getattr(signed, 'raw_transaction', None) or getattr(signed, 'rawTransaction')
    return raw.hex().removeprefix('0x')


def send_responses(provider, **overrides):
    responses = {
        'eth_blockNumber': hex(100),
        'eth_getBlockByNumber': {'baseFeePerGas': hex(10**9)},
        'eth_maxPriorityFeePerGas': hex(10**9),
        'eth_getCode': '0x',
        'eth_getTransactionCount': '0x0',
        'eth_getBalance': hex(10**19),
        'eth_estimateGas': hex(21_000 if isinstance(provider, EvmCoin) else 65_000),
        'eth_sendRawTransaction': TX,
    }
    if isinstance(provider, Erc20):
        responses['eth_call'] = f'0x{10 * amount_for(provider):064x}'  # balanceOf
    responses.update(overrides)
    return responses


# ─── every EVM row ──────────────────────────────────────────────────────────


@pytest.mark.parametrize('spec', EVM_SPECS, ids=by_id)
def test_network_guards(spec, monkeypatch):
    """A typo'd network must never become mainnet (real funds against test swaps), and an endpoint
    serving another chain must fail the boot rather than verify legs against the wrong chain."""
    prefix = get_chain_def(spec.chain_id).env_prefix
    provider = spec.cls()
    mainnet_id = provider.chain.chain_id
    wrong_id = next(i for n, i in provider.chain.network_def.chain_ids.items() if n != 'mainnet')
    rpc_stub(provider, {'eth_chainId': hex(wrong_id)})
    with pytest.raises(ConnectionError, match=f'expected {mainnet_id}'):
        provider.check_connection(require_send=False)

    monkeypatch.setenv(f'{prefix}_NETWORK', 'seplia')
    with pytest.raises(ValueError, match=f'{prefix}_NETWORK'):
        spec.cls()


@pytest.mark.parametrize('spec', EVM_SPECS, ids=by_id)
def test_only_a_settled_full_leg_from_the_pinned_sender_confirms(spec):
    provider = spec.cls()
    amount = amount_for(provider)

    rpc_stub(provider, mined(provider))
    info = provider.verify_transaction(TX, RECIPIENT, amount, expected_sender=TEST_ADDR)
    assert info is not None and info.confirmed and info.amount == amount and info.block_time == 0x64
    assert provider.verify_transaction(TX, RECIPIENT, amount, expected_sender=OTHER) is None

    # One block short of the row's depth is seen, but not yet confirmed. (Fresh providers: the
    # tip and the settled cache are per instance.)
    provider = spec.cls()
    rpc_stub(provider, mined(provider, confs=provider.chain_def.min_confirmations - 1))
    assert provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount).confirmed is False

    # Inclusion is not settlement; a short or misdirected payment pays nothing.
    for case in ({'status': '0x0'}, {'amount': amount - 1}, {'recipient': OTHER}):
        provider = spec.cls()
        rpc_stub(provider, mined(provider, **case))
        assert provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount) is None, case


@pytest.mark.parametrize('spec', EVM_SPECS, ids=by_id)
def test_an_unreadable_leg_is_unknown_not_absent(spec):
    """'Absent' is the verdict that slashes; a read that cannot complete must raise instead."""
    provider = spec.cls()
    amount = amount_for(provider)

    rpc_stub(provider, {'eth_getTransactionByHash': None})
    assert provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount) is None

    rpc_stub(provider, dict(mined(provider), eth_getTransactionReceipt=None))
    with pytest.raises(ProviderUnreachableError):
        provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount)

    rpc_stub(provider, dict(mined(provider), eth_getBlockByNumber={'hash': BLOCK_HASH}))
    with pytest.raises(ProviderUnreachableError):
        provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount)


@pytest.mark.parametrize('spec', EVM_SPECS, ids=by_id)
def test_send_needs_the_matching_key_and_then_broadcasts(spec, monkeypatch):
    provider = spec.cls()
    prefix = provider.chain_def.env_prefix
    amount = amount_for(provider) if isinstance(provider, Erc20) else 10**16
    assert provider.send_amount(RECIPIENT, amount) is None
    assert f'{prefix}_PRIVATE_KEY' in provider.last_send_error

    monkeypatch.setenv(f'{prefix}_PRIVATE_KEY', TEST_KEY)
    assert provider.send_amount(RECIPIENT, amount, from_address=RECIPIENT) is None
    assert 'key mismatch' in provider.last_send_error

    rpc_stub(provider, send_responses(provider))
    assert provider.send_amount(RECIPIENT, amount, from_address=TEST_ADDR) == (TX, 0)


@pytest.mark.parametrize('spec', NATIVE_SPECS, ids=by_id)
def test_native_send_signs_for_its_own_chain_at_the_fulfillment_gas_floor(spec, monkeypatch):
    # The whole signed payload: the row's chain id (a tx signed for another chain is invalid
    # here), EIP-1559 fees off the stubbed base fee (tip clamped to any protocol floor), and the
    # 100k fulfillment gas floor over a 21k estimate.
    monkeypatch.setenv(f'{get_chain_def(spec.chain_id).env_prefix}_PRIVATE_KEY', TEST_KEY)
    provider = spec.cls()
    broadcast = {}

    def capture(params):
        broadcast['raw'] = params[0]
        return TX

    rpc_stub(provider, send_responses(provider, eth_sendRawTransaction=capture))
    assert provider.send_amount(RECIPIENT, 10**16) == (TX, 0)
    tip = max(10**9, provider.chain.network_def.min_priority_fee_wei)
    expected = signed_raw(provider.chain.chain_id, 100_000, 2 * 10**9 + tip, tip)
    assert broadcast['raw'].removeprefix('0x') == expected


# ─── every token row ────────────────────────────────────────────────────────


@pytest.mark.parametrize('spec', TOKEN_SPECS, ids=by_id)
def test_only_a_transfer_log_from_the_pinned_contract_settles(spec):
    """Settlement truth is the pinned contract's Transfer log. Anything else that looks like a
    payment — another token's Transfer, an Approval, an event of the wrong arity, a pending call to
    another contract — must not settle the leg, on the first read or on the cached re-verify."""
    provider = spec.cls()
    amount = amount_for(provider)
    assert provider.token_contract == provider.chain_def.asset_locator
    if spec.chain_id in CIRCLE_NATIVE:
        assert provider.chain_def.asset_locator == CIRCLE_NATIVE[spec.chain_id]
    impostor = BRIDGED_TWINS.get(spec.chain_id, COUNTERFEIT)
    good = transfer_log(provider)
    decoys = (
        transfer_log(provider, contract=impostor),
        transfer_log(provider, topic0=APPROVAL_TOPIC0),
        dict(good, topics=[DELEGATE_VOTES_TOPIC0, topic(RECIPIENT)], data=f'0x{0:064x}{amount:064x}'),
        dict(good, topics=[*good['topics'], f'0x{7:064x}']),  # ERC-721 Transfer: same topic0, 4 topics
    )
    for decoy in decoys:
        rpc_stub(provider, mined(provider, logs=[decoy]))
        assert provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount) is None
        assert provider._settled_cache == {}
        assert provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount) is None

    # The real leg is found among impostors, and a double-paying decoy ordered first never outranks it.
    louder = transfer_log(provider, topic0=APPROVAL_TOPIC0, amount=2 * amount)
    rpc_stub(provider, mined(provider, logs=[louder, transfer_log(provider, contract=impostor), good]))
    assert provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount).amount == amount

    calldata = SEL_TRANSFER + topic(RECIPIENT).removeprefix('0x') + f'{amount:064x}'
    pending = {'hash': TX, 'from': TEST_ADDR.lower(), 'input': calldata, 'blockNumber': None}
    rpc_stub(provider, {'eth_getTransactionByHash': dict(pending, to=impostor)})
    assert provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount) is None
    rpc_stub(provider, {'eth_getTransactionByHash': dict(pending, to=provider.token_contract)})
    info = provider.fetch_matching_tx(TX, WIRE_RECIPIENT, amount)
    assert info.confirmed is False and info.amount == amount and info.sender == TEST_ADDR.lower()


@pytest.mark.parametrize('spec', TOKEN_SPECS, ids=by_id)
def test_a_test_network_uses_its_own_pin_or_refuses_to_build(spec, monkeypatch):
    """A token names its NETWORK's env, so one {PREFIX}_NETWORK moves it with every asset on that
    network. On a test network it takes its pinned test deployment or fails loudly naming the
    override — never the mainnet contract, which is how a testnet miner pays real tokens."""
    chain_def = get_chain_def(spec.chain_id)
    owner = next(c for c in SUPPORTED_CHAINS.values() if c.env_prefix == chain_def.env_prefix and c.networks)
    network = owner.testnet_network
    monkeypatch.setenv(f'{chain_def.env_prefix}_NETWORK', network)
    pinned = TESTNET_TOKEN_CONTRACTS.get(spec.chain_id, {}).get(network)
    if pinned is None:
        with pytest.raises(MissingTestnetDeployment, match=f'{spec.chain_id.upper()}_TOKEN_CONTRACT'):
            spec.cls()
        pinned = '0x' + '42' * 20
        monkeypatch.setenv(f'{spec.chain_id.upper()}_TOKEN_CONTRACT', pinned)
    token = spec.cls()
    assert token.token_contract == pinned
    assert token.chain.chain_id == token.chain.network_def.chain_ids[network]
    for native in NATIVE_SPECS:
        if get_chain_def(native.chain_id).env_prefix == chain_def.env_prefix:
            assert native.cls().chain.chain_id == token.chain.chain_id, native.chain_id


@pytest.mark.parametrize('spec', CHECKED_TOKENS, ids=by_id)
def test_each_declared_refusal_check_bounces_reserve_and_defers_the_slash(spec):
    """Only the row's declared selectors are probed (PAXG reverts on Circle's isBlacklisted, so
    probing it would defer every slash on the pair), and each one is positive evidence at both gates."""
    provider = spec.cls()
    declared = {selector for selector, _ in provider._checks}
    for refusing in declared:

        def views(method, params, refusing=refusing, **kw):
            if method == 'eth_blockNumber':
                return hex(1000)
            return f'0x{int(params[0]["data"].startswith(refusing)):064x}'

        provider.chain.eth_rpc = views
        assert provider.can_deliver_to(RECIPIENT, amount_for(provider)) is False
        assert provider.delivery_refused(RECIPIENT, since_unix=0) is True

    seen = []

    def clean(method, params, **kw):
        if method == 'eth_blockNumber':
            return hex(1000)
        seen.append(params[0]['data'][:10])
        return f'0x{0:064x}'

    provider.chain.eth_rpc = clean
    assert provider.can_deliver_to(RECIPIENT, amount_for(provider)) is True
    assert provider.delivery_refused(RECIPIENT, since_unix=0) is False
    assert set(seen) == declared


@pytest.mark.parametrize('spec', NO_CHECK_TOKENS, ids=by_id)
def test_a_token_with_no_freeze_surface_answers_without_the_rpc(spec):
    """Probing isBlacklisted/paused on these contracts reverts, and a revert out of delivery_refused
    defers every slash on the pair forever. Both gates and the boot answer without probing."""
    provider = spec.cls()
    calls = []
    provider.chain.eth_rpc = lambda method, params, **kw: calls.append(method)
    assert provider.can_deliver_to(RECIPIENT, amount_for(provider)) is True
    assert provider.delivery_refused(RECIPIENT, since_unix=0) is False
    assert provider.address_refused(RECIPIENT) is False
    assert calls == []

    def reverts(params):
        raise AssertionError('a token declaring no checks must not probe for one at boot')

    boot = {'eth_chainId': hex(provider.chain.chain_id), 'eth_blockNumber': '0x10', 'eth_getCode': '0x6080'}
    rpc_stub(provider, dict(boot, eth_call=reverts))
    provider.check_connection(require_send=False)


# ─── one row's own behaviour ────────────────────────────────────────────────


def test_paxg_minimum_leg_is_a_fraction_of_an_ounce():
    # One PAXG is a troy ounce of gold. A token COUNT copied from another row (UNI's 3) would make
    # the minimum leg >$12,000 and the pair would silently never route.
    assert CHAIN_PAXG.min_onchain_amount == 2_000_000_000_000_000  # 0.002 PAXG


@pytest.mark.parametrize('pair', (('baseusdc', 'sol'), ('sol', 'baseusdc')))
def test_six_decimal_token_rates_execute_in_both_directions(pair):
    # 6 decimals against the hub's 9: a real ~200 USDC/SOL market routes both ways, nonsense does not.
    assert is_executable_rate(200.0, *pair, 10_000_000, 100_000_000_000) is True
    assert is_executable_rate(1e-20, *pair, 10_000_000, 100_000_000_000) is False


@pytest.fixture
def frozen_now(monkeypatch):
    """Pin the clock so delivery_refused's span arithmetic lands on exact block offsets."""
    now = 1_800_000_000
    monkeypatch.setattr(evm_coin_module.time, 'time', lambda: now)
    return now


class TestBscDeliveryGates:
    """A BSC (or Polygon) dest can stop accepting a native transfer between reserve and payout by
    delegating via EIP-7702. The slash gate's temporal sampling is what spares the miner."""

    def test_slash_gate_exempts_a_dest_that_gained_then_revoked_code(self, frozen_now):
        # 'latest' is clean by the slash check; code lives at the midpoint offset alone, so a
        # passing run proves all three probe offsets were actually read.
        probed = []

        def code_at(params):
            probed.append(params[1])
            return '0xef0100' + '11' * 20 if params[1] == hex(9_970) else '0x'

        provider = rpc_stub(Bnb(), {'eth_blockNumber': hex(10_000), 'eth_getCode': code_at})
        assert provider.delivery_refused(RECIPIENT, frozen_now - 60) is True
        assert probed == ['latest', hex(9_940), hex(9_970)]

    def test_slash_gate_does_not_exempt_a_dest_that_never_had_code(self, frozen_now):
        provider = rpc_stub(Bnb(), {'eth_blockNumber': hex(10_000), 'eth_getCode': '0x'})
        assert provider.delivery_refused(RECIPIENT, frozen_now - 60) is False


def _fee_method_gone(params):
    raise EvmRpcError('the method eth_maxPriorityFeePerGas does not exist', {'code': -32601})


@pytest.mark.parametrize(
    'fee_answer',
    [_fee_method_gone, hex(FALLBACK_PRIORITY_FEE_WEI)],
    ids=['fee-method-absent', 'endpoint-suggests-below-the-floor'],
)
def test_polygon_tip_is_clamped_to_its_protocol_floor(monkeypatch, fee_answer):
    # Bor refuses a tx under 25 gwei at intake, so the generic 1 gwei fallback would sign something
    # no Polygon node accepts — deterministically, every pass, until every live pol swap times out.
    assert FALLBACK_PRIORITY_FEE_WEI < POLYGON.min_priority_fee_wei
    monkeypatch.setenv('POL_PRIVATE_KEY', TEST_KEY)
    base_fee = 250_000_000_000  # Polygon's real fee scale
    broadcast = {}

    def capture(params):
        broadcast['raw'] = params[0]
        return TX

    provider = Pol()
    rpc_stub(
        provider,
        send_responses(
            provider,
            eth_getBlockByNumber={'baseFeePerGas': hex(base_fee)},
            eth_maxPriorityFeePerGas=fee_answer,
            eth_sendRawTransaction=capture,
        ),
    )
    assert provider.send_amount(RECIPIENT, 10**16) == (TX, 0)
    floor = POLYGON.min_priority_fee_wei
    assert broadcast['raw'].removeprefix('0x') == signed_raw(137, 100_000, 2 * base_fee + floor, floor)


def test_cronos_mainnet_ladder_keeps_an_archive_rung_behind_the_fresh_one():
    # Only the official gateway serves the historical state the slash gate reads; publicnode keeps
    # ~100 blocks. The leader tracks the tip, the archive rung is reached by fall-through.
    assert Cro().chain.rpc_bases == ['https://cronos-evm-rpc.publicnode.com', 'https://evm.cronos.org']


def test_avalanche_rpc_urls_keep_the_c_chain_path(monkeypatch):
    # Ava Labs' gateways are path-bearing (/ext/bc/C/rpc); only a trailing slash may be trimmed.
    monkeypatch.setenv('AVAX_RPC_URLS', 'https://api.avax.network/ext/bc/C/rpc/,https://backup.example')
    assert Avax().chain.rpc_bases == ['https://api.avax.network/ext/bc/C/rpc', 'https://backup.example']


def test_avalanche_atomic_import_is_not_a_deposit():
    # A P/X→C atomic import credits an address from block extra data with no EVM transaction: real
    # but unverifiable (no tx hash for the confirm path), so the scanner must not claim it.
    atomic_block = {
        'transactions': [],
        'extDataGasUsed': '0x2bde',
        'extDataHash': '0x' + 'ef' * 32,
        'blockExtraData': '0x0000000000010000000000000001',
        'blockGasCost': '0x0',
        'timestampMilliseconds': '0x18f',
    }
    provider = rpc_stub(Avax(), {'eth_blockNumber': hex(10_000), 'eth_getBlockByNumber': atomic_block})
    assert provider.find_recent_outgoing(OTHER, RECIPIENT, 1) is None


def test_sub_second_chain_scan_is_bounded_and_parks_below_an_unreadable_block():
    # 5 min of HyperEVM blocks is ~300 sequential reads per first scan, past the public budget.
    provider = Hype()
    assert provider.SCAN_LOOKBACK_BLOCKS > MAX_WALK_BLOCKS
    scanned = []

    def block(params):
        scanned.append(int(params[0], 16))
        return {'transactions': []}

    rpc_stub(provider, {'eth_blockNumber': hex(10_000), 'eth_getBlockByNumber': block})
    assert provider.find_recent_outgoing(RECIPIENT, RECIPIENT, 1) is None
    assert len(scanned) == MAX_WALK_BLOCKS

    def boom(params):
        raise ConnectionError('down')

    provider = rpc_stub(Hype(), {'eth_blockNumber': hex(10_000), 'eth_getBlockByNumber': boom})
    assert provider.find_recent_outgoing(RECIPIENT, RECIPIENT, 1) is None
    # Never leap a block the scan could not read — the deposit in it must stay reachable.
    assert provider.scan_cursors[(RECIPIENT.lower(), RECIPIENT.lower(), 1)] == 10_000 - MAX_WALK_BLOCKS


def test_every_evm_chain_session_is_metered():
    """EVM traffic goes through the chain's session, not the asset's, so the meter must find it there."""
    providers = [spec.cls() for spec in EVM_SPECS]
    Meter([]).attach_chains(providers)
    for provider in providers:
        assert isinstance(provider.chain.http.get_adapter('https://rpc.example'), MeteredAdapter)

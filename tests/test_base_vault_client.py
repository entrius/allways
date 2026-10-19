"""BaseVaultClient — calldata, the EIP-2612 permit, and the refusals that keep a write from being sent."""

import pytest
from eth_account import Account

from allways.assets.evm import BASE, EvmRpcError
from allways.vault import base_client as bc
from allways.vault.base_client import BaseVaultClient, BaseVaultError

# Anvil's first dev key — public, throwaway.
KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'
OWNER = '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'
VAULT = '0x1111111111111111111111111111111111111111'
TOKEN = '0x036CbD53842c5426634e7929541eC2318f3dCF7e'  # Circle USDC on Base Sepolia
PUBKEY = bytes(range(1, 33))

# Permit vector from Foundry, independent of eth-account:
#   cast wallet sign --private-key $KEY --data --from-file permit.json
# over Permit(owner=OWNER, spender=VAULT, value=25_000_000, nonce=3, deadline=1_800_000_000),
# domain {name: "USDC", version: "2", chainId: 84532, verifyingContract: TOKEN}.
CAST_PERMIT_SIG = (
    '73c5cae1a4291cd864a69f8fe022f4e66a37c693b32406fce875bebcd2914ab6'
    '2eb60ee388ede8ed629a1b47cd514fcb83331f57323c593ac8ddbf76fc60e46a'
    '1c'
)


def _abi_string(text: str) -> str:
    body = text.encode()
    return '0x' + (32).to_bytes(32, 'big').hex() + len(body).to_bytes(32, 'big').hex() + body.ljust(32, b'\0').hex()


def _uint(value: int) -> str:
    return '0x' + value.to_bytes(32, 'big').hex()


class FakeChain:
    """Base Sepolia over a scripted JSON-RPC: records every call, answers from ``calls``."""

    network = 'sepolia'
    network_def = BASE
    chain_id = 84_532
    _key_env = 'BASE_PRIVATE_KEY'

    def __init__(self, eth_balance=10**18, estimate=None, calls=None, receipt_status='0x1', served_chain_id=84_532):
        self.log = []
        self.eth_balance = eth_balance
        self.estimate = estimate
        self.calls = calls or {}
        self.receipt_status = receipt_status
        self.served_chain_id = served_chain_id

    def _account(self):
        return Account.from_key(KEY)

    def sent(self):
        return [p for m, p in self.log if m == 'eth_sendRawTransaction']

    def estimates(self):
        return [p[0] for m, p in self.log if m == 'eth_estimateGas']

    def eth_rpc(self, method, params, **_):
        self.log.append((method, params))
        if method == 'eth_call':
            to, data = params[0]['to'].lower(), params[0]['data']
            return self.calls[(to, data[:10])]
        if method == 'eth_estimateGas':
            if isinstance(self.estimate, Exception):
                raise self.estimate
            return hex(self.estimate or 100_000)
        if method == 'eth_getBalance':
            return hex(self.eth_balance)
        if method == 'eth_getBlockByNumber':
            return {'baseFeePerGas': hex(10_000_000)}
        if method == 'eth_maxPriorityFeePerGas':
            return hex(1_000_000)
        if method == 'eth_getTransactionCount':
            return '0x5'
        if method == 'eth_chainId':
            return hex(self.served_chain_id)
        if method == 'eth_sendRawTransaction':
            return '0x' + 'ab' * 32
        if method == 'eth_getTransactionReceipt':
            return {'status': self.receipt_status} if self.receipt_status else None
        raise AssertionError(f'unexpected rpc {method}')


def _token_calls(over=None):
    calls = {
        (VAULT.lower(), '0x' + bc.SEL_USDC.hex()): '0x' + bytes(12).hex() + TOKEN[2:].lower(),
        (TOKEN.lower(), '0x' + bc.SEL_NAME.hex()): _abi_string('USDC'),
        (TOKEN.lower(), '0x' + bc.SEL_VERSION.hex()): _abi_string('2'),
        (TOKEN.lower(), '0x' + bc.SEL_NONCES.hex()): _uint(3),
        (TOKEN.lower(), '0x' + bc.SEL_ALLOWANCE.hex()): _uint(0),
    }
    calls.update(over or {})
    return calls


def _client(chain, **kw):
    return BaseVaultClient(chain, VAULT, sleep=lambda _: None, **kw)


# ─── selectors: pinned against `forge inspect AllwaysUsdcVault methodIdentifiers / errors` ───


@pytest.mark.parametrize(
    'sel,expected',
    [
        (bc.SEL_GET_BOND, '0d8912f3'),
        (bc.SEL_LOCK_BOND, '9a70250c'),
        (bc.SEL_MAX_COLLATERAL, '251f47c1'),
        (bc.SEL_MIN_COLLATERAL, 'ba2de9bc'),
        (bc.SEL_POST_COLLATERAL, '50f2012f'),
        (bc.SEL_POST_COLLATERAL_WITH_PERMIT, '23ef7c37'),
        (bc.SEL_USDC, '3e413bee'),
        (bc.SEL_WITHDRAW_COLLATERAL, '6112fe2e'),
        # ERC-20 / EIP-2612 standard selectors.
        (bc.SEL_APPROVE, '095ea7b3'),
        (bc.SEL_ALLOWANCE, 'dd62ed3e'),
        (bc.SEL_BALANCE_OF, '70a08231'),
        (bc.SEL_NONCES, '7ecebe00'),
    ],
)
def test_selectors_match_the_foundry_build(sel, expected):
    assert sel.hex() == expected


@pytest.mark.parametrize(
    'data,name',
    [
        ('0x1caa378c', 'BondLocked'),
        ('0x3a23d825', 'InsufficientCollateral'),
        ('0x23f34a73', 'ExceedsMaxCollateral'),
        ('0xef8a0996', 'LockStateUnchanged'),
        ('0x2c5211c6', 'InvalidAmount'),
        ('0x42afbf19', 'InvalidSolanaPubkey'),
        ('0xed554234' + '00' * 64, 'DepositMismatch'),
    ],
)
def test_vault_reverts_decode_to_their_error_names(data, name):
    assert bc.decode_revert(data) == name


def test_a_token_require_string_decodes_too():
    data = '0x08c379a0' + _abi_string('ERC20: transfer amount exceeds balance')[2:]
    assert bc.decode_revert(data) == 'ERC20: transfer amount exceeds balance'


# ─── calldata ────────────────────────────────────────────────────────────────


def test_lock_bond_calldata_is_selector_plus_the_raw_pubkey():
    chain = FakeChain()
    _client(chain).lock_bond(PUBKEY)
    est = chain.estimates()[0]
    assert est['to'] == VAULT and est['from'] == OWNER
    assert est['data'] == '0x9a70250c' + PUBKEY.hex()
    assert len(chain.sent()) == 1


def test_withdraw_and_post_calldata_carry_the_amount_as_one_word():
    chain = FakeChain()
    client = _client(chain)
    client.withdraw_collateral(25_000_000)
    client.post_collateral(7)
    assert chain.estimates()[0]['data'] == '0x6112fe2e' + (25_000_000).to_bytes(32, 'big').hex()
    assert chain.estimates()[1]['data'] == '0x50f2012f' + (7).to_bytes(32, 'big').hex()


def test_the_signed_transaction_carries_the_simulated_calldata_on_the_right_chain():
    from eth_account.typed_transactions import TypedTransaction
    from hexbytes import HexBytes

    chain = FakeChain()
    _client(chain).lock_bond(PUBKEY)
    raw = HexBytes(chain.sent()[0][0])
    tx = TypedTransaction.from_bytes(raw).as_dict()
    assert tx['chainId'] == 84_532
    assert bytes(tx['to']).hex() == VAULT[2:].lower()
    assert bytes(tx['data']).hex() == '9a70250c' + PUBKEY.hex()
    assert tx['gas'] == 120_000  # 100k estimate + 20% headroom
    assert Account.recover_transaction(raw) == OWNER


def test_get_bond_decodes_all_four_fields():
    ret = _uint(30_000_000) + _uint(1)[2:] + _uint(4)[2:] + PUBKEY.hex()
    chain = FakeChain(calls={(VAULT.lower(), '0x0d8912f3'): ret})
    bond = _client(chain).get_bond(OWNER)
    assert (bond.collateral, bond.locked, bond.epoch, bond.solana_pubkey) == (30_000_000, True, 4, PUBKEY)
    assert chain.log[0][1][0]['data'] == '0x0d8912f3' + bytes(12).hex() + OWNER[2:].lower()


# ─── the permit ──────────────────────────────────────────────────────────────


def test_permit_signature_matches_the_foundry_vector():
    typed = bc.permit_typed_data(TOKEN, 'USDC', '2', 84_532, OWNER, VAULT, 25_000_000, 3, 1_800_000_000)
    v, r, s = bc.sign_permit(KEY, typed)
    assert (r + s + bytes([v])).hex() == CAST_PERMIT_SIG


def test_post_with_permit_reads_the_domain_from_chain_and_encodes_v_r_s():
    chain = FakeChain(calls=_token_calls())
    _client(chain).post_collateral_with_permit(25_000_000, deadline=1_800_000_000)
    data = bytes.fromhex(chain.estimates()[0]['data'][2:])
    sig = bytes.fromhex(CAST_PERMIT_SIG)
    assert data[:4].hex() == '23ef7c37'
    assert int.from_bytes(data[4:36], 'big') == 25_000_000
    assert int.from_bytes(data[36:68], 'big') == 1_800_000_000
    assert int.from_bytes(data[68:100], 'big') == sig[64]
    assert data[100:132] == sig[:32] and data[132:164] == sig[32:64]
    assert ('eth_chainId', []) in chain.log


def test_a_permit_is_never_signed_for_a_chain_the_rpc_does_not_serve():
    chain = FakeChain(calls=_token_calls(), served_chain_id=8_453)
    with pytest.raises(BaseVaultError, match='chain id'):
        _client(chain).post_collateral_with_permit(25_000_000, deadline=1_800_000_000)
    assert chain.sent() == []


# ─── refusals before signing ─────────────────────────────────────────────────


def test_a_write_the_signer_cannot_pay_gas_for_is_never_sent():
    # max fee = 2 * 0.01 gwei + 0.001 gwei = 21_000_000 wei; 120k gas needs 2.52e12 wei.
    chain = FakeChain(eth_balance=2_000_000_000_000)
    with pytest.raises(BaseVaultError, match='Not enough Base ETH'):
        _client(chain).lock_bond(PUBKEY)
    assert chain.sent() == []


def test_a_simulated_revert_is_named_and_nothing_is_sent():
    chain = FakeChain(
        estimate=EvmRpcError('revert', {'code': 3, 'message': 'execution reverted', 'data': '0x1caa378c'})
    )
    with pytest.raises(BaseVaultError) as err:
        _client(chain).withdraw_collateral(1)
    assert err.value.revert == 'BondLocked'
    assert chain.sent() == []


def test_approve_path_checks_gas_for_both_txs_before_the_approve():
    # Enough for the approve alone (120k gas) but not for approve + the post budget.
    chain = FakeChain(eth_balance=4_000_000_000_000, calls=_token_calls())
    with pytest.raises(BaseVaultError, match='Not enough Base ETH'):
        _client(chain).approve_and_post(25_000_000)
    assert chain.sent() == []


def test_approve_path_approves_exactly_the_amount_then_posts():
    chain = FakeChain(calls=_token_calls())
    results = _client(chain).approve_and_post(25_000_000)
    assert [r.ok for r in results] == [True, True]
    approve, post = chain.estimates()
    assert approve['to'] == TOKEN
    assert approve['data'] == '0x095ea7b3' + bytes(12).hex() + VAULT[2:] + (25_000_000).to_bytes(32, 'big').hex()
    assert post['data'].startswith('0x50f2012f')


def test_approve_path_skips_the_approve_when_the_allowance_already_covers_it():
    calls = _token_calls({(TOKEN.lower(), '0x' + bc.SEL_ALLOWANCE.hex()): _uint(10**9)})
    chain = FakeChain(calls=calls)
    _client(chain).approve_and_post(25_000_000)
    assert len(chain.sent()) == 1
    assert chain.estimates()[0]['data'].startswith('0x50f2012f')


def test_a_zero_pubkey_is_refused_locally():
    chain = FakeChain()
    with pytest.raises(BaseVaultError):
        _client(chain).lock_bond(bytes(32))
    assert chain.log == []


def test_an_outstanding_receipt_reports_unmined_rather_than_failed():
    ticks = iter(range(0, 1000, 10))
    chain = FakeChain(receipt_status=None)
    result = BaseVaultClient(chain, VAULT, sleep=lambda _: None, clock=lambda: next(ticks)).lock_bond(PUBKEY)
    assert result.mined is False and result.tx_hash == '0x' + 'ab' * 32


def test_the_vault_address_must_be_an_evm_address(monkeypatch):
    monkeypatch.setenv('ALLWAYS_BASE_VAULT_ADDRESS', '5Fkn2rNGvWxZ3cMNWbbT3FVrsyBjWmpE5fU4yYGDCrKAfLhs')
    from allways.vault.client import VaultConfigError

    with pytest.raises(VaultConfigError, match='0x plus 40 hex'):
        bc.resolve_base_vault_address({})

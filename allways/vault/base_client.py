"""BaseVaultClient — every call into the ``AllwaysUsdcVault`` Solidity contract on Base.

The Base twin of ``BondVaultClient``. Today the ``alw base-vault`` CLI drives the miner surface
(post / lock / withdraw); the validator relay extends this same client with its vote rounds.

Transport is the baseusdc spoke's own Base ``EvmChain``: one BASE_NETWORK, one BASE_RPC_URLS
ladder and one BASE_PRIVATE_KEY, so the bond and the spoke can never disagree about which Base
they are on. Configuration: ``ALLWAYS_BASE_VAULT_ADDRESS`` / the ``base-vault-address`` config key.

Every write is simulated first (``eth_estimateGas``) and refused before signing when it would
revert or when the signer's ETH can't cover gas at the current max fee — a send never burns gas
discovering either on-chain.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Callable, Optional, Sequence, Tuple

from eth_account import Account
from eth_utils import keccak, to_checksum_address

from allways.assets.evm import EVM_NETWORKS, FALLBACK_PRIORITY_FEE_WEI, EvmChain, EvmRpcError
from allways.chains import CHAIN_BASEUSDC
from allways.vault.client import VaultConfigError

_ADDR_RE = re.compile(r'0x[0-9a-fA-F]{40}')


def selector(signature: str) -> bytes:
    return keccak(text=signature)[:4]


# Computed from the ABI signatures, never transcribed (tests pin them against the Foundry build).
SEL_POST_COLLATERAL = selector('postCollateral(uint256)')
SEL_POST_COLLATERAL_WITH_PERMIT = selector('postCollateralWithPermit(uint256,uint256,uint8,bytes32,bytes32)')
SEL_LOCK_BOND = selector('lockBond(bytes32)')
SEL_WITHDRAW_COLLATERAL = selector('withdrawCollateral(uint256)')
SEL_GET_BOND = selector('getBond(address)')
SEL_MIN_COLLATERAL = selector('minCollateral()')
SEL_MAX_COLLATERAL = selector('maxCollateral()')
SEL_USDC = selector('usdc()')

SEL_APPROVE = selector('approve(address,uint256)')
SEL_ALLOWANCE = selector('allowance(address,address)')
SEL_BALANCE_OF = selector('balanceOf(address)')
SEL_NONCES = selector('nonces(address)')
SEL_NAME = selector('name()')
SEL_VERSION = selector('version()')

# Every custom error the vault declares, so a simulated revert names itself instead of a 4-byte blob.
VAULT_ERRORS = (
    'NotValidator()',
    'InvalidValidatorSet()',
    'AlreadyValidator()',
    'NotPendingValidator()',
    'AdmissionVoid()',
    'SelfRemoval()',
    'AlreadyVoted()',
    'PendingConflict()',
    'InvalidAmount()',
    'InsufficientCollateral()',
    'ExceedsMaxCollateral()',
    'BondLocked()',
    'LockStateUnchanged()',
    'EpochMismatch()',
    'AlreadySlashed()',
    'InvalidReimbursement()',
    'NoPendingSlash()',
    'InvalidBatch()',
    'ConfigOutOfBounds(uint8)',
    'ZeroAddress()',
    'InvalidToken()',
    'InvalidPayee()',
    'BatchNotSorted()',
    'DepositMismatch(uint256,uint256)',
    'InvalidSolanaPubkey()',
    'InvalidLotRef()',
    'LotAlreadyOpen(bytes32)',
    'LotAlreadySettled()',
    'LotDeadlinePassed()',
    'LotDeadlineTooFar()',
    'LotExceedsPot(uint256,uint256)',
    'LotExceedsCap()',
    'RecipientNotBonded()',
    'RecipientUnderCovered()',
    'DeadManNotElapsed(uint64)',
    'InsufficientGas()',
)
ERROR_NAMES = {selector(sig): sig.split('(')[0] for sig in VAULT_ERRORS}
_SEL_ERROR_STRING = selector('Error(string)')

# eth_estimateGas plus this share as headroom; the unused part is refunded.
GAS_HEADROOM_DIVISOR = 5
# What a postCollateral costs when it can't be simulated yet (before its approve lands). Measured
# well under this on the Foundry gas snapshot; it only sizes the up-front ETH check.
POST_COLLATERAL_GAS_BUDGET = 150_000
RECEIPT_TIMEOUT_SECS = 90
RECEIPT_POLL_SECS = 2
# Long enough to survive a slow confirm prompt, short enough that a leaked permit goes stale.
PERMIT_TTL_SECS = 1800


class BaseVaultError(Exception):
    """A write refused before signing: it would revert (``revert`` names the error), or the signer
    cannot pay for gas. The message is user-facing."""

    def __init__(self, message: str, revert: Optional[str] = None):
        super().__init__(message)
        self.revert = revert


@dataclass(frozen=True)
class BaseBond:
    collateral: int
    locked: bool
    epoch: int
    solana_pubkey: bytes


@dataclass
class BaseCallResult:
    ok: bool
    tx_hash: str
    # False when no receipt had landed by RECEIPT_TIMEOUT_SECS: the tx may still mine.
    mined: bool = True


def resolve_base_vault_address(config=None) -> str:
    addr = os.environ.get('ALLWAYS_BASE_VAULT_ADDRESS') or (config.get('base-vault-address') if config else None)
    if not addr:
        raise VaultConfigError(
            'Base vault address not configured — `alw config set base-vault-address <0x…>` '
            'or set ALLWAYS_BASE_VAULT_ADDRESS'
        )
    if not _ADDR_RE.fullmatch(addr):
        raise VaultConfigError(f'Base vault address must be 0x plus 40 hex characters (got "{addr}")')
    return addr


def base_chain() -> EvmChain:
    """The baseusdc spoke's Base network: BASE_NETWORK / BASE_RPC_URLS / BASE_PRIVATE_KEY."""
    return EvmChain(EVM_NETWORKS[CHAIN_BASEUSDC.host_chain], CHAIN_BASEUSDC.env_prefix)


def word(value: int) -> bytes:
    return int(value).to_bytes(32, 'big')


def address_word(address: str) -> bytes:
    return bytes(12) + bytes.fromhex(address[2:])


def encode_call(sel: bytes, *words: bytes) -> bytes:
    return sel + b''.join(words)


def decode_revert(data) -> Optional[str]:
    """The revert reason from an eth_call/estimateGas error's ``data``: a vault error name, an
    ``Error(string)`` message (the token's own require), or None when it's neither."""
    if isinstance(data, dict):
        data = data.get('data')
    if not isinstance(data, str) or not data.startswith('0x') or len(data) < 10:
        return None
    raw = bytes.fromhex(data[2:])
    name = ERROR_NAMES.get(raw[:4])
    if name:
        return name
    if raw[:4] == _SEL_ERROR_STRING and len(raw) >= 68:
        length = int.from_bytes(raw[36:68], 'big')
        return raw[68 : 68 + length].decode('utf-8', 'replace')
    return None


def decode_string(raw: bytes) -> str:
    """An ABI-encoded dynamic ``string`` return value."""
    offset = int.from_bytes(raw[:32], 'big')
    length = int.from_bytes(raw[offset : offset + 32], 'big')
    return raw[offset + 32 : offset + 32 + length].decode('utf-8')


def permit_typed_data(
    token: str, name: str, version: str, chain_id: int, owner: str, spender: str, value: int, nonce: int, deadline: int
) -> dict:
    """The EIP-2612 ``Permit`` as EIP-712 typed data, domain exactly as the token reports it."""
    return {
        'types': {
            'EIP712Domain': [
                {'name': 'name', 'type': 'string'},
                {'name': 'version', 'type': 'string'},
                {'name': 'chainId', 'type': 'uint256'},
                {'name': 'verifyingContract', 'type': 'address'},
            ],
            'Permit': [
                {'name': 'owner', 'type': 'address'},
                {'name': 'spender', 'type': 'address'},
                {'name': 'value', 'type': 'uint256'},
                {'name': 'nonce', 'type': 'uint256'},
                {'name': 'deadline', 'type': 'uint256'},
            ],
        },
        'primaryType': 'Permit',
        'domain': {
            'name': name,
            'version': version,
            'chainId': chain_id,
            'verifyingContract': to_checksum_address(token),
        },
        'message': {
            'owner': to_checksum_address(owner),
            'spender': to_checksum_address(spender),
            'value': value,
            'nonce': nonce,
            'deadline': deadline,
        },
    }


def sign_permit(key, typed: dict) -> Tuple[int, bytes, bytes]:
    signed = Account.sign_typed_data(key, full_message=typed)
    return signed.v, signed.r.to_bytes(32, 'big'), signed.s.to_bytes(32, 'big')


class BaseVaultClient:
    def __init__(
        self,
        chain: EvmChain,
        address: str,
        account=None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ):
        self.chain = chain
        self.address = to_checksum_address(address)
        # The signer: BASE_PRIVATE_KEY by default. The relay passes its validator key.
        self.account = account if account is not None else chain._account()
        self._sleep = sleep
        self._clock = clock
        self._usdc: Optional[str] = None

    @classmethod
    def from_config(cls, config=None, account=None, **kwargs) -> 'BaseVaultClient':
        """Build from the env/config keys. Raises ``VaultConfigError`` when the vault isn't set up."""
        return cls(base_chain(), resolve_base_vault_address(config), account=account, **kwargs)

    @property
    def signer_address(self) -> Optional[str]:
        return self.account.address if self.account is not None else None

    # ─── transport ───────────────────────────────────────────────────────────

    def eth_call(self, to: str, data: bytes) -> bytes:
        try:
            result = self.chain.eth_rpc('eth_call', [{'to': to, 'data': '0x' + data.hex()}, 'latest'])
        except EvmRpcError as e:
            reason = decode_revert(e.error.get('data'))
            raise BaseVaultError(f'Read reverted on {to}{f": {reason}" if reason else ""}', reason) from e
        except Exception as e:
            raise BaseVaultError(f'Base RPC read failed: {e}') from e
        return bytes.fromhex((result or '0x')[2:])

    def _uint(self, to: str, data: bytes) -> int:
        raw = self.eth_call(to, data)
        if len(raw) < 32:
            raise BaseVaultError(f'Unexpected empty return from {to} — is it the right contract on this network?')
        return int.from_bytes(raw[:32], 'big')

    def chain_id(self) -> int:
        """The id the RPC actually serves, checked against BASE_NETWORK — signing a permit or a tx
        for the wrong chain id would be refused, or worse, valid somewhere else."""
        served = int(self.chain.eth_rpc('eth_chainId', []), 16)
        if served != self.chain.chain_id:
            raise BaseVaultError(
                f'Base RPC serves chain id {served}, but BASE_NETWORK={self.chain.network} expects {self.chain.chain_id}'
            )
        return served

    def eth_balance(self, address: str) -> int:
        return int(self.chain.eth_rpc('eth_getBalance', [address, 'latest']), 16)

    def fees(self) -> Tuple[int, int]:
        """``(max_fee, tip)`` per gas, priced like the spoke's own sends."""
        latest = self.chain.eth_rpc('eth_getBlockByNumber', ['latest', False])
        base_fee = int((latest or {}).get('baseFeePerGas') or '0x0', 16)
        try:
            tip = int(self.chain.eth_rpc('eth_maxPriorityFeePerGas', []), 16)
        except Exception:
            tip = FALLBACK_PRIORITY_FEE_WEI
        tip = max(tip, self.chain.network_def.min_priority_fee_wei)
        return 2 * base_fee + tip, tip

    def estimate_gas(self, to: str, data: bytes) -> int:
        """Simulate as the signer: the gas limit with headroom, or ``BaseVaultError`` naming the revert."""
        params = {'from': self._signer().address, 'to': to, 'data': '0x' + data.hex()}
        try:
            est = int(self.chain.eth_rpc('eth_estimateGas', [params]), 16)
        except EvmRpcError as e:
            if not e.is_execution_revert:
                raise BaseVaultError(f'Base RPC could not simulate the call: {e}') from e
            reason = decode_revert(e.error.get('data')) or decode_revert(e.error)
            raise BaseVaultError(f'The call would revert{f": {reason}" if reason else ""}', reason) from e
        except Exception as e:
            raise BaseVaultError(f'Base RPC could not simulate the call: {e}') from e
        return est + est // GAS_HEADROOM_DIVISOR

    def require_gas(self, gas: int, max_fee: int) -> None:
        """Refuse when the signer's ETH can't cover ``gas`` at ``max_fee`` — before anything is signed."""
        signer = self._signer().address
        need = gas * max_fee
        have = self.eth_balance(signer)
        if have < need:
            raise BaseVaultError(
                f'Not enough Base ETH for gas: {signer} holds {have / 1e18:.8f} ETH, this needs up to '
                f'{need / 1e18:.8f} ETH ({gas} gas at {max_fee / 1e9:.4f} gwei). Fund it on Base '
                f'{self.chain.network} and retry.'
            )

    def submit(self, to: str, data: bytes, gas: Optional[int] = None) -> BaseCallResult:
        """Simulate, check gas, sign, broadcast and wait for the receipt."""
        acct = self._signer()
        gas = gas or self.estimate_gas(to, data)
        max_fee, tip = self.fees()
        self.require_gas(gas, max_fee)
        nonce = int(self.chain.eth_rpc('eth_getTransactionCount', [acct.address, 'pending']), 16)
        signed = Account.sign_transaction(
            {
                'chainId': self.chain.chain_id,
                'nonce': nonce,
                'to': to_checksum_address(to),
                'value': 0,
                'data': '0x' + data.hex(),
                'gas': gas,
                'maxFeePerGas': max_fee,
                'maxPriorityFeePerGas': tip,
            },
            acct.key,
        )
        raw = getattr(signed, 'raw_transaction', None) or getattr(signed, 'rawTransaction')
        expected = '0x' + signed.hash.hex().removeprefix('0x')
        tx_hash = self.chain.eth_rpc('eth_sendRawTransaction', ['0x' + raw.hex().removeprefix('0x')], timeout=30)
        return self.wait(tx_hash or expected)

    def wait(self, tx_hash: str) -> BaseCallResult:
        deadline = self._clock() + RECEIPT_TIMEOUT_SECS
        while True:
            try:
                receipt = self.chain.eth_rpc('eth_getTransactionReceipt', [tx_hash])
            except Exception:
                receipt = None
            if receipt is not None:
                return BaseCallResult(ok=int(receipt.get('status') or '0x0', 16) == 1, tx_hash=tx_hash)
            if self._clock() >= deadline:
                return BaseCallResult(ok=False, tx_hash=tx_hash, mined=False)
            self._sleep(RECEIPT_POLL_SECS)

    def _signer(self):
        if self.account is None:
            raise VaultConfigError(f'No Base signer — set {self.chain._key_env} (the baseusdc spoke key)')
        return self.account

    # ─── vault reads ─────────────────────────────────────────────────────────

    def get_bond(self, miner: str) -> BaseBond:
        raw = self.eth_call(self.address, encode_call(SEL_GET_BOND, address_word(miner)))
        if len(raw) < 128:
            raise BaseVaultError(f'Unexpected getBond return from {self.address} — is it the vault on this network?')
        return BaseBond(
            collateral=int.from_bytes(raw[0:32], 'big'),
            locked=int.from_bytes(raw[32:64], 'big') != 0,
            epoch=int.from_bytes(raw[64:96], 'big'),
            solana_pubkey=raw[96:128],
        )

    def min_collateral(self) -> int:
        return self._uint(self.address, SEL_MIN_COLLATERAL)

    def max_collateral(self) -> int:
        """0 is UNLIMITED in the contract, not closed."""
        return self._uint(self.address, SEL_MAX_COLLATERAL)

    def usdc_address(self) -> str:
        """The token the vault was built against — read from the vault, never assumed."""
        if self._usdc is None:
            raw = self.eth_call(self.address, SEL_USDC)
            if len(raw) < 32:
                raise BaseVaultError(f'Unexpected usdc() return from {self.address}')
            self._usdc = to_checksum_address('0x' + raw[12:32].hex())
        return self._usdc

    # ─── token reads ─────────────────────────────────────────────────────────

    def usdc_balance(self, owner: str) -> int:
        return self._uint(self.usdc_address(), encode_call(SEL_BALANCE_OF, address_word(owner)))

    def usdc_allowance(self, owner: str) -> int:
        return self._uint(
            self.usdc_address(), encode_call(SEL_ALLOWANCE, address_word(owner), address_word(self.address))
        )

    def permit_nonce(self, owner: str) -> int:
        return self._uint(self.usdc_address(), encode_call(SEL_NONCES, address_word(owner)))

    def token_domain(self) -> Tuple[str, str]:
        """``(name, version)`` exactly as the token reports them — the permit domain is never hardcoded."""
        token = self.usdc_address()
        try:
            return decode_string(self.eth_call(token, SEL_NAME)), decode_string(self.eth_call(token, SEL_VERSION))
        except (BaseVaultError, ValueError, UnicodeDecodeError) as e:
            raise BaseVaultError(f'Cannot read the permit domain from {token} ({e}) — deposit with --approve') from e

    # ─── miner writes ────────────────────────────────────────────────────────

    def approve(self, amount: int) -> BaseCallResult:
        return self.submit(self.usdc_address(), encode_call(SEL_APPROVE, address_word(self.address), word(amount)))

    def post_collateral(self, amount: int) -> BaseCallResult:
        """Post ``amount`` against an allowance that is already in place."""
        return self.submit(self.address, encode_call(SEL_POST_COLLATERAL, word(amount)))

    def approve_and_post(self, amount: int) -> Sequence[BaseCallResult]:
        """The two-tx fallback. Gas for BOTH is checked before the approve is signed, so a miner
        never ends up with an allowance and no ETH left to use it."""
        owner = self._signer().address
        if self.usdc_allowance(owner) >= amount:
            return [self.post_collateral(amount)]
        token = self.usdc_address()
        approve_data = encode_call(SEL_APPROVE, address_word(self.address), word(amount))
        approve_gas = self.estimate_gas(token, approve_data)
        max_fee, _ = self.fees()
        self.require_gas(approve_gas + POST_COLLATERAL_GAS_BUDGET, max_fee)
        first = self.submit(token, approve_data, gas=approve_gas)
        if not first.ok:
            return [first]
        return [first, self.post_collateral(amount)]

    def permit_signature(self, amount: int, deadline: int) -> Tuple[int, bytes, bytes]:
        acct = self._signer()
        name, version = self.token_domain()
        typed = permit_typed_data(
            self.usdc_address(),
            name,
            version,
            self.chain_id(),
            acct.address,
            self.address,
            amount,
            self.permit_nonce(acct.address),
            deadline,
        )
        return sign_permit(acct.key, typed)

    def post_collateral_with_permit(self, amount: int, deadline: Optional[int] = None) -> BaseCallResult:
        """One tx: an EIP-2612 permit for exactly ``amount`` signed here, then the post."""
        deadline = deadline if deadline is not None else int(self._clock()) + PERMIT_TTL_SECS
        v, r, s = self.permit_signature(amount, deadline)
        return self.submit(
            self.address,
            encode_call(SEL_POST_COLLATERAL_WITH_PERMIT, word(amount), word(deadline), word(v), r, s),
        )

    def lock_bond(self, solana_pubkey: bytes) -> BaseCallResult:
        if len(solana_pubkey) != 32 or not any(solana_pubkey):
            raise BaseVaultError('Solana pubkey must be 32 non-zero bytes', 'InvalidSolanaPubkey')
        return self.submit(self.address, encode_call(SEL_LOCK_BOND, bytes(solana_pubkey)))

    def withdraw_collateral(self, amount: int) -> BaseCallResult:
        return self.submit(self.address, encode_call(SEL_WITHDRAW_COLLATERAL, word(amount)))

import os
import re
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Tuple
from urllib.parse import urlparse

import bittensor as bt
import requests
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_utils import is_checksum_address

from allways.assets.asset import Asset, ProviderUnreachableError
from allways.assets.chain import Chain

_HEX_ADDR_RE = re.compile(r'^0x[0-9a-fA-F]{40}$')


class EvmRpcError(RuntimeError):
    """A JSON-RPC ``error`` object from an endpoint (an execution/method result), distinct from a
    transport failure. Carries the structured error so callers branch on the code, not the message
    wording (which varies by provider). ``is_execution_revert`` is a deterministic on-chain verdict."""

    def __init__(self, message: str, error: Any):
        super().__init__(message)
        self.error = error if isinstance(error, dict) else {}

    @property
    def is_execution_revert(self) -> bool:
        # Geth: code 3 + 'execution reverted'; others: -32000 with 'revert' in the message.
        return self.error.get('code') == 3 or 'revert' in str(self.error.get('message') or '').lower()


# eth_maxPriorityFeePerGas fallback when an endpoint doesn't serve it (1 gwei clears
# comfortably in normal conditions without meaningfully overpaying a transfer).
FALLBACK_PRIORITY_FEE_WEI = 1_000_000_000

# Send-dedup / deposit-scan window in wall seconds (≈5 min); each chain derives its block
# bound from its own block time. Note seconds_per_block is an integer floor, so on sub-second
# chains (Arbitrum ~0.25s → floored to 1) the bound covers ~4× fewer wall-seconds than this.
# Harmless: the scanner only surfaces a hash the confirm path re-verifies by exact tx hash.
SCAN_LOOKBACK_SECS = 300


def rpc_tag(base: str) -> str:
    """Short, log-friendly host label for an RPC endpoint (e.g. 'publicnode', 'drpc')."""
    host = (urlparse(base).netloc or base).split(':')[0].removeprefix('www.')
    parts = host.split('.')
    return parts[-2] if len(parts) >= 2 else host


@dataclass(frozen=True)
class EvmNetwork:
    """An EVM family's per-network facts: canonical chain ids + keyless default RPC ladders.

    Networks within the family (mainnet/sepolia/…) are keys of these maps; families
    themselves (Ethereum, Arbitrum, …) are INSTANCES of this row — never subclasses.
    eth_chainId is checked against the configured network at startup, so a wrong-network
    RPC fails fast instead of verifying the wrong chain. Keyed/paid endpoints embed their
    key in the URL path (via {PREFIX}_RPC_URLS), so no header plumbing is needed.
    """

    label: str
    chain_ids: Mapping[str, int]
    rpc_urls: Mapping[str, Tuple[str, ...]]


ETHEREUM = EvmNetwork(
    label='Ethereum',
    chain_ids={'mainnet': 1, 'sepolia': 11_155_111},
    rpc_urls={
        'mainnet': ('https://ethereum-rpc.publicnode.com', 'https://eth.drpc.org'),
        # drpc's free tier stopped serving Sepolia (rpc error 35), leaving the ladder
        # single-endpoint — a null then never reaches quorum and every absent tx raises.
        'sepolia': ('https://ethereum-sepolia-rpc.publicnode.com', 'https://sepolia.gateway.tenderly.co'),
    },
)
ARBITRUM = EvmNetwork(
    label='Arbitrum',
    chain_ids={'mainnet': 42_161, 'sepolia': 421_614},
    rpc_urls={
        'mainnet': ('https://arbitrum-one-rpc.publicnode.com', 'https://arbitrum.drpc.org'),
        'sepolia': ('https://arbitrum-sepolia-rpc.publicnode.com', 'https://arbitrum-sepolia.drpc.org'),
    },
)
HYPERLIQUID = EvmNetwork(
    label='Hyperliquid',
    chain_ids={'mainnet': 999, 'testnet': 998},
    rpc_urls={
        # The official gateways are rate-limited to 100 req/min per IP — fine as the keyless
        # default, but operators should point {PREFIX}_RPC_URLS at a keyed endpoint.
        'mainnet': ('https://rpc.hyperliquid.xyz/evm', 'https://hyperliquid.drpc.org'),
        'testnet': ('https://rpc.hyperliquid-testnet.xyz/evm', 'https://hyperliquid-testnet.drpc.org'),
    },
)
BSC = EvmNetwork(
    label='BNB Smart Chain',
    chain_ids={'mainnet': 56, 'testnet': 97},  # testnet = Chapel
    rpc_urls={
        # Picked on a real eth_getTransactionReceipt under load, not on eth_chainId: publicnode's
        # BSC mainnet node answers chainId and then 403s every receipt ('Archive requests require a
        # personal token') even one block back, and the receipt IS the settlement check. drpc's free
        # BSC tier 429s under any burst, which would strand null quorum. Both excluded on purpose.
        'mainnet': ('https://bsc-dataseed.bnbchain.org', 'https://bsc.blockrazor.xyz'),
        # publicnode's TESTNET node does serve receipts — the asymmetry with mainnet is deliberate.
        'testnet': ('https://bsc-testnet-dataseed.bnbchain.org', 'https://bsc-testnet-rpc.publicnode.com'),
    },
)

AVALANCHE = EvmNetwork(
    label='Avalanche',
    chain_ids={'mainnet': 43_114, 'fuji': 43_113},
    # Ava Labs' gateway leads because it is the only rung serving historical eth_getCode, which
    # delivery_refused probes on every overdue swap — publicnode prunes state, drpc times out.
    rpc_urls={
        'mainnet': (
            'https://api.avax.network/ext/bc/C/rpc',
            'https://avalanche-c-chain-rpc.publicnode.com',
            'https://avalanche.drpc.org',
        ),
        'fuji': (
            'https://api.avax-test.network/ext/bc/C/rpc',
            'https://avalanche-fuji-c-chain-rpc.publicnode.com',
            'https://avalanche-fuji.drpc.org',
        ),
    },
)

BASE = EvmNetwork(
    label='Base',
    chain_ids={'mainnet': 8_453, 'sepolia': 84_532},
    rpc_urls={
        # publicnode is deliberately absent: its free Base mainnet endpoint 403s every
        # eth_getTransactionReceipt as an "archive request", so it can never settle a leg.
        # The official gateways rate-limit under load (429s observed on mainnet.base.org),
        # which makes an absent tx raise instead of resolving; operators running volume
        # should point {PREFIX}_RPC_URLS at a keyed endpoint, as on Hyperliquid.
        'mainnet': ('https://mainnet.base.org', 'https://base.drpc.org'),
        'sepolia': ('https://sepolia.base.org', 'https://base-sepolia.drpc.org'),
    },
)

CRONOS = EvmNetwork(
    label='Cronos',
    chain_ids={'mainnet': 25, 'testnet': 338},
    rpc_urls={
        # The official gateway leads as the only mainnet rung serving historical eth_getCode, which
        # delivery_refused probes up to 120 blocks back on every overdue swap: publicnode prunes
        # Ethermint state at tip-107 and errors below it (measured 2026-08-12). publicnode stays
        # second — it serves receipts, blocks and tx lookups cleanly, which is what null quorum needs.
        'mainnet': ('https://evm.cronos.org', 'https://cronos-evm-rpc.publicnode.com'),
        # Both testnet rungs serve historical eth_getCode past 100k blocks; official leads for symmetry.
        'testnet': ('https://evm-t3.cronos.org', 'https://cronos-testnet.drpc.org'),
    },
)

# ChainDefinition.host_chain → the EvmNetwork that hosts the asset.
EVM_NETWORKS: Mapping[str, EvmNetwork] = {
    'ethereum': ETHEREUM,
    'arbitrum': ARBITRUM,
    'hyperliquid': HYPERLIQUID,
    'bsc': BSC,
    'avalanche': AVALANCHE,
    'base': BASE,
    'cronos': CRONOS,
}


class EvmChain(Chain):
    """An EVM network: JSON-RPC transport (failover ladder, null quorum), hex/EIP-55
    addresses, EIP-191 ownership proofs, tip reads.

    One behavior class for the whole family; per-network facts come from the `EvmNetwork`
    config row, env wiring from ``env_prefix`` — the NETWORK's identity, so every asset on
    a network reads one {PREFIX}_NETWORK and one ladder and they cannot disagree.

    Addresses are hex and case-insensitive (EIP-55 is a display checksum), so every
    comparison goes through ``normalize_address`` (lowercase) — on-chain strings keep
    whatever casing the user committed.
    """

    def __init__(self, network_def: EvmNetwork, env_prefix: str):
        self.network_def = network_def
        self.env_prefix = env_prefix
        net_var = f'{env_prefix}_NETWORK'
        self.network = os.environ.get(net_var, '').lower()
        if not self.network:
            self.network = 'mainnet'
            testnet = next((n for n in network_def.chain_ids if n != 'mainnet'), None)
            bt.logging.warning(f'{net_var} unset — defaulting to mainnet; set {net_var}={testnet} for testnet.')
        if self.network not in network_def.chain_ids:
            # A typo silently becoming mainnet would pay real funds against test swaps.
            raise ValueError(f'Unknown {net_var} {self.network!r} — expected one of {list(network_def.chain_ids)}')

        # Same rationale as the BTC provider: long-running validators + pooled idle TLS
        # sockets that public CDNs silently drop → wedged reads until timeout.
        self.http = requests.Session()
        self.http.headers['Connection'] = 'close'

        raw = os.environ.get(f'{env_prefix}_RPC_URLS', '')
        self.rpc_bases = [u.strip().rstrip('/') for u in raw.split(',') if u.strip()] or list(
            network_def.rpc_urls[self.network]
        )

    @property
    def chain_id(self) -> int:
        return self.network_def.chain_ids[self.network]

    @property
    def _key_env(self) -> str:
        return f'{self.env_prefix}_PRIVATE_KEY'

    def eth_rpc(
        self,
        method: str,
        params: list,
        timeout: int = 15,
        null_needs_quorum: bool = False,
        bases: Optional[Tuple[str, ...]] = None,
    ) -> Any:
        """Call ``method`` against each endpoint in order, narrating every failover.

        A JSON-RPC ``error`` object is treated as endpoint trouble (rate limit, pruned
        state, method gap) and falls through; ``result: null`` is authoritative data
        ("no such tx") and is returned as None. Raises the last error when all fail.

        ``null_needs_quorum``: return None only when every endpoint reachably agrees;
        null + an unreachable endpoint raises. For paths where "absent" costs money —
        one stale node in a public load balancer must not read as "never happened".

        ``bases``: restrict the call to specific endpoints (startup per-endpoint probes);
        None = the configured ladder.
        """
        rpc_bases = list(bases) if bases else self.rpc_bases
        log = f'{self.network_def.label}Rpc'
        payload = {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}
        last_err: Optional[Exception] = None
        revert_err: Optional[EvmRpcError] = None
        saw_null = False
        for i, base in enumerate(rpc_bases):
            pos = f'[{i + 1}/{len(rpc_bases)}]'
            tag = rpc_tag(base)
            nxt = rpc_tag(rpc_bases[i + 1]) if i + 1 < len(rpc_bases) else None
            tail = f'falling back to: {nxt}' if nxt else 'no providers left, giving up'
            try:
                resp = self.http.post(base, json=payload, timeout=timeout)
            except Exception as e:
                last_err = e
                bt.logging.warning(f'{log} {pos} {tag} {method} → request error: {e}; {tail}')
                continue

            if resp.status_code != 200:
                last_err = requests.HTTPError(f'{base} {method}: {resp.status_code}', response=resp)
                bt.logging.warning(f'{log} {pos} {tag} {method} → HTTP {resp.status_code}; {tail}')
                continue

            try:
                body = resp.json()
            except ValueError as e:
                last_err = e
                bt.logging.warning(f'{log} {pos} {tag} {method} → bad JSON: {e}; {tail}')
                continue

            if 'error' in body:
                err = body['error']
                last_err = EvmRpcError(f'{base} {method}: rpc error {err}', err)
                # A revert is a deterministic execution verdict — remember it so it outranks any
                # transport error from a later endpoint (else a flaky node masks a real refusal).
                if last_err.is_execution_revert:
                    revert_err = last_err
                bt.logging.warning(f'{log} {pos} {tag} {method} → rpc error {err}; {tail}')
                continue

            result = body.get('result')
            if result is None and null_needs_quorum:
                saw_null = True
                bt.logging.debug(f'{log} {pos} {tag} {method} → null; seeking quorum from remaining endpoint(s)')
                continue
            if i > 0:
                bt.logging.info(f'{log} {pos} {tag} {method} → ok (served after {i} fallback(s))')
            else:
                bt.logging.debug(f'{log} {pos} {tag} {method} → ok')
            return result
        if revert_err is not None:
            raise revert_err
        if saw_null and last_err is None:
            return None
        if saw_null:
            raise ProviderUnreachableError(f'{method}: null without quorum (an endpoint was unreachable): {last_err}')
        raise last_err or RuntimeError(f'all {self.network_def.label} RPCs failed')

    def connect_network(self) -> Tuple[int, int]:
        """(chain_id, tip); raises ConnectionError when no endpoint answers or ANY answering
        endpoint serves the wrong network. EVERY configured endpoint is probed: a wrong-network
        URL deeper in the ladder would otherwise surface only mid-outage, quietly serving
        wrong-chain verifications. An unreachable endpoint just warns — flaky ≠ misconfigured."""
        chain_id = None
        for base in self.rpc_bases:
            try:
                served = int(self.eth_rpc('eth_chainId', [], timeout=10, bases=(base,)), 16)
            except Exception as e:
                bt.logging.warning(f'{self.env_prefix} endpoint {rpc_tag(base)} unreachable at startup: {e}')
                continue
            if served != self.chain_id:
                raise ConnectionError(
                    f'{self.env_prefix} endpoint {rpc_tag(base)} serves chain id {served}, expected {self.chain_id} '
                    f'for {self.network} — {self.env_prefix}_RPC_URLS and {self.env_prefix}_NETWORK disagree'
                )
            chain_id = served
        if chain_id is None:
            raise ConnectionError(f'Cannot reach any {self.network_def.label} RPC')
        try:
            tip = int(self.eth_rpc('eth_blockNumber', [], timeout=10), 16)
        except Exception as e:
            raise ConnectionError(f'Cannot reach {self.network_def.label} RPC: {e}') from e
        return chain_id, tip

    def get_current_block_height(self) -> Optional[int]:
        try:
            return int(self.eth_rpc('eth_blockNumber', [], timeout=10), 16)
        except Exception as e:
            bt.logging.debug(f'{self.env_prefix} get_current_block_height failed: {e}')
            return None

    def normalize_address(self, address: str) -> str:
        return address.lower() if isinstance(address, str) else address

    def is_valid_address(self, address: str) -> bool:
        """0x + 40 hex, and if mixed-case the EIP-55 checksum must verify (typo protection,
        no RPC). Done by hand: eth_utils 5+ ``is_address`` accepts unprefixed hex and stopped
        validating checksums, which would wave through exactly the typos EIP-55 exists to catch."""
        if not isinstance(address, str) or not _HEX_ADDR_RE.match(address):
            return False
        # The zero address is never a real destination: ETH burns to it, and an ERC-20 transfer()
        # reverts on it — a dest no delivery gate catches, so an honest miner would be slashed.
        if int(address, 16) == 0:
            return False
        body = address[2:]
        if body == body.lower() or body == body.upper():
            return True
        return is_checksum_address(address)

    def _account(self, key: Optional[Any] = None):
        raw = key if isinstance(key, str) and key else os.environ.get(self._key_env)
        if not raw:
            return None
        try:
            return Account.from_key(raw)
        except Exception as e:
            bt.logging.error(f'{self.env_prefix} private key unusable: {e}')
            return None

    def sign_from_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        """EIP-191 personal_sign over ``message``. key: hex private key; None → {PREFIX}_PRIVATE_KEY."""
        acct = self._account(key)
        if acct is None:
            bt.logging.error(f'{self.env_prefix} signing requires the {self._key_env} env var (or an explicit key)')
            return ''
        if self.normalize_address(acct.address) != self.normalize_address(address):
            bt.logging.error(
                f'{self.env_prefix} key derives {acct.address} but the proof address is {address} — key mismatch'
            )
            return ''
        try:
            signed = Account.sign_message(encode_defunct(text=message), acct.key)
            return signed.signature.hex()
        except Exception as e:
            bt.logging.error(f'{self.env_prefix} sign_from_proof failed: {e}')
            return ''

    def verify_from_proof(self, address: str, message: str, signature: str) -> bool:
        """Recover the EIP-191 signer and compare case-insensitively. No RPC dependency."""
        try:
            sig = signature if signature.startswith('0x') else f'0x{signature}'
            recovered = Account.recover_message(encode_defunct(text=message), signature=sig)
            return self.normalize_address(recovered) == self.normalize_address(address)
        except Exception as e:
            bt.logging.error(f'{self.env_prefix} verify_from_proof failed: {e}')
            return False


class EvmAsset(Asset):
    """Base for assets on EVM chains: key handling, send dedup ladder, deposit-scan
    cursors, the settled-tx cache, and the connection check scaffold.

    Chain questions route through ``self.chain`` (seam rule): every EVM asset, coin or
    token, binds ``self._chain`` to its host network's `EvmChain` at construction. Env
    vars key off that NETWORK's ``env_prefix``, shared by every asset on it.
    """

    # Settled-tx cache bound — comfortably above any realistic concurrent-leg count.
    # Deliberately NOT cleared per pass: entries are keyed by immutable block hash
    # (reorg-safe), and cross-pass reuse is the cache's whole point.
    _SETTLED_CACHE_MAX = 512
    _MAX_SCAN_CURSORS = 64

    def __init__(self):
        # Asset-scoped log tag (the wire id). Transport lines carry the CHAIN's tag instead —
        # one Arbitrum ladder serves every Arbitrum asset, so the two must stay distinguishable.
        self._log = f'[{self.chain_def.id}]'
        self.last_send_error: Optional[str] = None
        # Settled-tx cache, keyed tx_hash → immutable per-block facts. A mined tx's receipt
        # and its block's timestamp are immutable per block hash, so the validator's 12s
        # re-verify pays 1 RPC instead of 3 — parity with BTC's single /tx. Reorg-safe: an
        # entry is served only while the fresh tx fetch reports the SAME blockHash; a reorg
        # changes it → miss → full refetch. Only fully-settled entries are cached.
        self._settled_cache: OrderedDict[str, dict] = OrderedDict()
        # Own broadcasts, tx_hash → (to, amount, dedup scope, head at broadcast): send dedup
        # scoped to this process, ONE obligation (the swap key — v3.1 fund-safety: two concurrent
        # identical payouts must both send) and SCAN_LOOKBACK_BLOCKS (#461 class).
        self.broadcasted_txids: dict[str, Tuple[str, int, str, int]] = {}
        # Deposit-scanner head cursors, keyed per (from, to, amount) — see find_recent_outgoing.
        self.scan_cursors: dict[Tuple[str, str, int], int] = {}
        self.SCAN_LOOKBACK_BLOCKS = max(1, SCAN_LOOKBACK_SECS // self.chain_def.seconds_per_block)

    def _send_error(self, msg: str) -> None:
        self.last_send_error = msg
        bt.logging.error(msg)

    def describe(self) -> str:
        chain = self.chain
        hosts = ', '.join(urlparse(base).netloc or base for base in chain.rpc_bases)
        return f'{chain.network_def.label} JSON-RPC ({chain.network}): {hosts}'

    def can_send_from(self, address: str) -> bool:
        acct = self.chain._account()
        norm = self.chain.normalize_address
        return acct is not None and norm(acct.address) == norm(address)

    def check_connection(self, require_send: bool = True) -> None:
        if require_send:
            key_env = self.chain._key_env
            key = os.environ.get(key_env)
            if not key:
                raise ConnectionError(f'{self.chain_def.env_prefix} signing requires the {key_env} env var')
            try:
                Account.from_key(key)
            except Exception as e:
                raise ConnectionError(f'{key_env} is not a valid 32-byte hex key: {e}') from e
        chain_id, tip = self.chain.connect_network()
        bt.logging.success(
            f'[{self.chain.network_def.label}Rpc] connected: '
            f'network={self.chain.network}, chain_id={chain_id}, tip={tip}'
        )

    def _prior_broadcast(self, want: Tuple[str, int, str], head: int) -> Optional[str]:
        """Reusable tx hash from a prior own broadcast to (to, amount, dedup scope); None when a
        fresh send is provably safe; raises when an in-flight duplicate can't be ruled out. Reuse
        requires the tx to be in the mempool or settled (status 1) — a reverted tx moved no funds
        and clears. Keyed on the SWAP, not the payout shape: two concurrent swaps with identical
        (to, amount) never collide. Entries age out after SCAN_LOOKBACK_BLOCKS."""
        for txid, (to_norm, amt, scope, seen) in list(self.broadcasted_txids.items()):
            if head - seen > self.SCAN_LOOKBACK_BLOCKS:
                del self.broadcasted_txids[txid]
                continue
            if (to_norm, amt, scope) != want:
                continue
            tx = self.chain.eth_rpc('eth_getTransactionByHash', [txid], null_needs_quorum=True)
            if tx is None:
                continue
            if tx.get('blockNumber') is None:
                return txid
            receipt = self.chain.eth_rpc('eth_getTransactionReceipt', [txid])
            if receipt is None:
                raise RuntimeError(f'prior tx {txid[:16]}... mined but its receipt is unavailable')
            if int(receipt.get('status') or '0x0', 16) == 1:
                return txid
            del self.broadcasted_txids[txid]
        return None

    def _set_cursor(self, key: Tuple[str, str, int], height: int) -> None:
        self.scan_cursors[key] = height
        if len(self.scan_cursors) > self._MAX_SCAN_CURSORS:
            self.scan_cursors.pop(next(iter(self.scan_cursors)))

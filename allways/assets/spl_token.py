import os
from typing import List, Optional

import bittensor as bt
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from allways.assets.asset import (
    Asset,
    MissingTestnetDeployment,
    ProviderUnreachableError,
    SendResult,
    TransactionInfo,
)
from allways.assets.sol import LOG_SOL, RESERVED_ACCOUNTS, SolanaChain
from allways.chains import ChainDefinition
from allways.constants import CANCEL_REASON_SOL_RESERVED, CANCEL_REASON_SPL_FROZEN
from allways.solana.rpc import SolanaRpc, TransientRpcError, classify_cluster

# Legacy SPL Token — Circle's USDC mint lives here, not under Token-2022. Pinned rather than read
# from the mint so a Token-2022 look-alike mint can never satisfy a USDC leg.
TOKEN_PROGRAM_ID = Pubkey.from_string('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA')
ASSOCIATED_TOKEN_PROGRAM_ID = Pubkey.from_string('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL')
SYSTEM_PROGRAM_ID = Pubkey.from_string('11111111111111111111111111111111')
# SPL Token instruction tags (spl_token::instruction::TokenInstruction discriminants).
IX_TRANSFER_CHECKED = 12
# Associated-token-program: CreateIdempotent — a no-op when the ATA already exists, so a send
# can always prepend it instead of probing first.
IX_CREATE_ATA_IDEMPOTENT = 1
# Rent-exempt minimum for a 165-byte token account plus one signature's fee: what the miner must
# hold in SOL beside the token balance to deliver to a wallet with no ATA yet.
ATA_RENT_LAMPORTS = 2_039_280
TX_FEE_LAMPORTS = 5_000

# Issuer deployments per (asset id, cluster) beside the mainnet mint in the registry row. Keyed by
# the genesis-hash cluster name — Solana has no {PREFIX}_NETWORK; the cluster is whatever the RPC
# serves. {ASSET_ID}_TOKEN_MINT overrides either (e2e fakes on localnet, emergency repoint).
CLUSTER_MINTS = {
    # Circle-verified USDC on devnet (developers.circle.com). No Circle USDC exists on testnet.
    'solusdc': {'devnet': '4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU'},
}


def associated_token_address(owner: str, mint: Pubkey) -> Pubkey:
    """The canonical token account for (owner, mint) — where a plain wallet receives the token."""
    ata, _ = Pubkey.find_program_address(
        [bytes(Pubkey.from_string(owner)), bytes(TOKEN_PROGRAM_ID), bytes(mint)], ASSOCIATED_TOKEN_PROGRAM_ID
    )
    return ata


def create_ata_idempotent_ix(payer: Pubkey, owner: Pubkey, mint: Pubkey) -> Instruction:
    return Instruction(
        ASSOCIATED_TOKEN_PROGRAM_ID,
        bytes([IX_CREATE_ATA_IDEMPOTENT]),
        [
            AccountMeta(payer, is_signer=True, is_writable=True),
            AccountMeta(associated_token_address(str(owner), mint), is_signer=False, is_writable=True),
            AccountMeta(owner, is_signer=False, is_writable=False),
            AccountMeta(mint, is_signer=False, is_writable=False),
            AccountMeta(SYSTEM_PROGRAM_ID, is_signer=False, is_writable=False),
            AccountMeta(TOKEN_PROGRAM_ID, is_signer=False, is_writable=False),
        ],
    )


def transfer_checked_ix(
    src: Pubkey, mint: Pubkey, dst: Pubkey, owner: Pubkey, amount: int, decimals: int
) -> Instruction:
    """TransferChecked binds the mint and decimals into the instruction, so a wrong-mint or
    wrong-precision send fails on-chain instead of moving the wrong asset."""
    data = bytes([IX_TRANSFER_CHECKED]) + int(amount).to_bytes(8, 'little') + bytes([decimals])
    return Instruction(
        TOKEN_PROGRAM_ID,
        data,
        [
            AccountMeta(src, is_signer=False, is_writable=True),
            AccountMeta(mint, is_signer=False, is_writable=False),
            AccountMeta(dst, is_signer=False, is_writable=True),
            AccountMeta(owner, is_signer=True, is_writable=False),
        ],
    )


def _parsed_info(account: Optional[dict]) -> Optional[dict]:
    """``data.parsed.info`` of a jsonParsed account, or None when absent/unparsed."""
    if not account:
        return None
    data = account.get('data')
    if not isinstance(data, dict):
        return None
    return (data.get('parsed') or {}).get('info')


class SplToken(Asset):
    """Swap-leg provider for an SPL token on Solana, bound by a registry row (``host_chain='solana'``,
    ``asset_locator`` = the mainnet mint). The Solana sibling of ``Erc20``.

    Addresses are OWNER wallets, never token accounts: the leg lands in the owner's associated
    token account, derived here, so the user-facing address is the same pubkey as for native SOL.
    Verification diffs ``meta.pre/postTokenBalances`` at that ATA for the pinned mint; the sender is
    the owner of the debited token account (the fee payer may be someone else entirely).

    Refusal surface: the issuer can freeze a token account (USDC's mint carries a freeze authority).
    A frozen destination ATA cannot be credited through no fault of the miner — it bounces the
    reservation and, at slash time, is no-fault-cancel evidence (``CANCEL_REASON_SPL_FROZEN``).
    """

    def __init__(
        self,
        chain_def: ChainDefinition,
        solana_rpc_url: Optional[str] = None,
        solana_keypair: Optional[Keypair] = None,
        timeout: int = 30,
    ):
        self._chain_def = chain_def
        self._chain = SolanaChain(solana_rpc_url, solana_keypair, timeout=timeout)
        # Resolved lazily: the cluster is only knowable from the RPC's genesis hash, and construction
        # must stay offline (create_assets builds every provider before any check).
        self._mint: Optional[Pubkey] = None

    @property
    def chain_def(self) -> ChainDefinition:
        return self._chain_def

    @property
    def rpc(self) -> SolanaRpc:
        return self.chain.rpc

    @rpc.setter
    def rpc(self, value) -> None:
        self.chain.rpc = value

    @property
    def keypair(self) -> Optional[Keypair]:
        return self.chain.keypair

    def describe(self) -> str:
        mint = str(self._mint) if self._mint is not None else '(mint unresolved)'
        return f'{self.chain_def.name} {mint} via {self.chain.describe()}'

    def can_send_from(self, address: str) -> bool:
        return self.chain.can_send_from(address)

    # --- mint resolution ---

    @property
    def mint(self) -> Pubkey:
        """The token mint for the cluster this RPC serves: env override, else the cluster's pinned
        deployment (mainnet = the registry row's asset_locator). An unknown cluster (localnet) with no
        override raises MissingTestnetDeployment — there is no USDC there to swap."""
        if self._mint is None:
            self._mint = Pubkey.from_string(self._resolve_mint())
        return self._mint

    def _resolve_mint(self) -> str:
        var = f'{self.chain_def.id.upper()}_TOKEN_MINT'
        override = os.environ.get(var)
        if override:
            return override
        cluster = classify_cluster(self.rpc.get_genesis_hash())
        if cluster == 'mainnet':
            return self.chain_def.asset_locator
        mint = CLUSTER_MINTS.get(self.chain_def.id, {}).get(cluster)
        if not mint:
            raise MissingTestnetDeployment(f'No {self.chain_def.id} mint on the {cluster} cluster — set {var}')
        return mint

    def ata(self, owner: str) -> Pubkey:
        return associated_token_address(owner, self.mint)

    # --- connection ---

    def check_connection(self, require_send: bool = True, **kwargs) -> None:
        if require_send and self.keypair is None:
            raise ConnectionError(f'{self.chain_def.id} send requires a Solana keypair (pass solana_keypair)')
        slot = self.chain.check_rpc()
        mint = self.mint
        try:
            account = self.rpc.get_parsed_account(mint)
        except Exception as e:
            # A probe we can't COMPLETE is transient (rate-limit, flaky endpoint) — degrade, as Erc20 does; a
            # raise here drops the spoke for the process lifetime and it then votes TIMEOUT past max_extend_at.
            # Unlike eth_getCode, a null account for a years-old mint IS a real fault, so only this path degrades.
            bt.logging.warning(f'{self.chain_def.id} mint probe unreachable ({e}) — degraded')
            return
        if account is None:
            raise ConnectionError(f'{self.chain_def.id} mint {mint} does not exist on {self.chain.rpc_url}')
        if account.get('owner') != str(TOKEN_PROGRAM_ID):
            raise ConnectionError(
                f'{self.chain_def.id} mint {mint} is not a legacy SPL Token mint (owner {account.get("owner")})'
            )
        decimals = (_parsed_info(account) or {}).get('decimals')
        if decimals != self.chain_def.decimals:
            raise ConnectionError(
                f'{self.chain_def.id} mint {mint} has {decimals} decimals, registry says {self.chain_def.decimals}'
            )
        if require_send:
            me = str(self.keypair.pubkey())
            if self.get_balance(me) == 0:
                bt.logging.warning(
                    f'{LOG_SOL} {self.chain_def.id}: signer {me} holds no {self.chain_def.name} — dest legs cannot be paid'
                )
        bt.logging.success(f'{LOG_SOL} {self.chain_def.id} connected: slot={slot} mint={mint}')

    # --- verification ---

    def fetch_matching_tx(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,  # unused — Solana indexes by signature
        max_scan_blocks: int = 150,  # unused
    ) -> Optional[TransactionInfo]:
        if not tx_hash:
            return None
        try:
            tx = self.rpc.get_transaction(tx_hash)
        except TransientRpcError as e:
            # SolanaRpc._call never lets requests errors escape — it re-raises them as TransientRpcError
            # (incl. SolanaRpcUnreachable). Catching the wrong type here fell through to `return None`,
            # i.e. an RPC outage read as "no such payment" — slash-eligible.
            raise ProviderUnreachableError(f'Solana RPC unreachable: {e}') from e
        except Exception as e:
            bt.logging.error(f'{LOG_SOL} getTransaction failed for {tx_hash[:16]}...: {e}')
            return None
        if not tx:
            bt.logging.debug(f'{LOG_SOL} tx {tx_hash[:16]}... not found')
            return None
        return self._build_tx_info(tx_hash, tx, expected_recipient, expected_amount)

    def _build_tx_info(
        self, tx_hash: str, tx: dict, expected_recipient: str, expected_amount: int
    ) -> Optional[TransactionInfo]:
        meta = tx.get('meta') or {}
        if meta.get('err') is not None:
            bt.logging.debug(f'{LOG_SOL} tx {tx_hash[:16]}... failed on-chain (err={meta.get("err")})')
            return None
        keys = self.chain.account_keys(tx, meta)
        credit, sender = self._token_movement(keys, meta, expected_recipient)
        if credit is None or credit < expected_amount:
            bt.logging.warning(
                f'{LOG_SOL} tx {tx_hash[:16]}... credits {expected_recipient} {credit} {self.chain_def.native_unit} '
                f'(< {expected_amount} required)'
            )
            return None
        slot = tx.get('slot')
        confirmations = self.chain.confirmations(slot)
        return TransactionInfo(
            tx_hash=tx_hash,
            confirmed=confirmations >= self.chain_def.min_confirmations,
            sender=sender,
            recipient=expected_recipient,
            amount=credit,
            block_number=slot,
            confirmations=confirmations,
            block_time=tx.get('blockTime'),  # unix seconds, the replay-freshness floor (B2)
        )

    def _token_movement(self, keys: List[str], meta: dict, recipient: str) -> tuple[Optional[int], str]:
        """(net credit to recipient's ATA in the pinned mint, owner of the debited token account).
        Credit is None when the ATA isn't in the tx. A mined tx whose token-balance arrays are missing
        is 'unknown', never 'no such payment' — that verdict would slash a paying miner."""
        pre = meta.get('preTokenBalances')
        post = meta.get('postTokenBalances')
        if pre is None or post is None:
            raise ProviderUnreachableError('transaction meta lacks token balances — cannot judge the leg')
        mint = str(self.mint)
        ata = str(self.ata(recipient))
        if ata not in keys:
            return None, ''
        idx = keys.index(ata)

        def amounts(entries: list) -> dict[int, tuple[int, str]]:
            out: dict[int, tuple[int, str]] = {}
            for e in entries or []:
                if e.get('mint') != mint:
                    continue
                amt = int(((e.get('uiTokenAmount') or {}).get('amount')) or 0)
                out[int(e.get('accountIndex'))] = (amt, e.get('owner') or '')
            return out

        pre_by, post_by = amounts(pre), amounts(post)
        credit = post_by.get(idx, (0, ''))[0] - pre_by.get(idx, (0, ''))[0]
        # The sender is whoever's token account went down. Ambiguous (none, or several owners) fails
        # closed as '' — the validator's sender pin then rejects rather than guesses.
        debited = {
            (pre_by[i][1] or post_by.get(i, (0, ''))[1])
            for i in set(pre_by) | set(post_by)
            if post_by.get(i, (0, ''))[0] < pre_by.get(i, (0, ''))[0]
        }
        debited.discard('')
        sender = next(iter(debited)) if len(debited) == 1 else ''
        return credit, sender

    def find_recent_outgoing(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        """Signature of a recent tx from ``from_addr`` crediting ``to_addr``'s ATA >= ``amount``, else
        None. A hash-finder for deposit detection; ``verify_transaction`` stays the sole verifier."""
        try:
            sigs = self.rpc.get_signatures_for_address(str(self.ata(to_addr)), limit=20)
        except Exception as e:
            bt.logging.debug(f'{LOG_SOL} find_recent_outgoing signature scan failed: {e}')
            return None
        for entry in sigs or []:
            sig = entry.get('signature')
            if not sig or entry.get('err') is not None:
                continue
            try:
                tx = self.rpc.get_transaction(sig)
            except Exception:
                continue
            if not tx:
                continue
            meta = tx.get('meta') or {}
            if meta.get('err') is not None:
                continue
            try:
                credit, sender = self._token_movement(self.chain.account_keys(tx, meta), meta, to_addr)
            except ProviderUnreachableError:
                continue
            if sender == from_addr and credit is not None and credit >= amount:
                return sig
        return None

    # --- balances + delivery gates ---

    def _token_account(self, owner: str) -> Optional[dict]:
        """Parsed info of the owner's ATA, or None when it doesn't exist. Raises on RPC trouble."""
        return _parsed_info(self.rpc.get_parsed_account(self.ata(owner)))

    def get_balance(self, address: str) -> int:
        """Token balance of ``address``'s ATA in the smallest unit (0 when the ATA doesn't exist)."""
        try:
            info = self._token_account(address)
        except Exception as e:
            bt.logging.error(f'{LOG_SOL} {self.chain_def.id} get_balance failed for {address}: {e}')
            return 0
        if not info:
            return 0
        return int(((info.get('tokenAmount') or {}).get('amount')) or 0)

    def _frozen(self, address: str) -> bool:
        info = self._token_account(address)
        return bool(info) and info.get('state') == 'frozen'

    def _is_token_account(self, address: str) -> bool:
        """A token account pasted where an OWNER wallet belongs: deliverable in principle, but to a
        different ATA than the one derived from it — the leg would never verify."""
        account = self.rpc.get_parsed_account(address)
        return bool(account) and account.get('owner') == str(TOKEN_PROGRAM_ID)

    def can_deliver_to(self, address: str, amount: int, from_address: Optional[str] = None) -> bool:
        """Reserve-time gate: a reserved key can't own an ATA, a frozen ATA can't be credited, and a
        token account is not an owner. Fails open on RPC trouble (mirrors Erc20)."""
        if address in RESERVED_ACCOUNTS:
            return False
        try:
            return not (self._frozen(address) or self._is_token_account(address))
        except Exception:
            return True

    def delivery_refused(self, address: str, since_unix: int) -> bool:
        """Slash gate: a frozen destination ATA is issuer refusal — positive evidence, never slash over
        it. An RPC failure raises so the caller defers: a flaky RPC postpones a slash, never falsifies
        one. Freezes persist until the issuer thaws, so the live probe is the load-bearing signal."""
        if address in RESERVED_ACCOUNTS:
            return True
        return self._frozen(address)

    def cancel_evidence(
        self, address: str, amount: int, tx_hash: Optional[str] = None, from_address: Optional[str] = None
    ) -> Optional[int]:
        if address in RESERVED_ACCOUNTS:
            return CANCEL_REASON_SOL_RESERVED
        try:
            return CANCEL_REASON_SPL_FROZEN if self._frozen(address) else None
        except Exception as e:
            bt.logging.warning(f'{LOG_SOL} {self.chain_def.id} cancel_evidence probe failed for {address}: {e}')
            return None

    # --- send ---

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None, dedup_key: Optional[str] = None
    ) -> SendResult:
        """Deliver ``amount`` (smallest unit) to ``to_address``'s ATA, creating it if missing (miner pays
        the rent). TransferChecked pins mint + decimals on-chain; the broadcast loop and double-send
        guard are the chain's."""
        if self.keypair is None:
            bt.logging.error(f'{self.chain_def.id} send_amount called on a read-only provider (no keypair)')
            return None
        me = self.keypair.pubkey()
        if from_address is not None and str(me) != str(from_address):
            bt.logging.error(f'{LOG_SOL} committed sender {from_address} != wallet {me} — not sending')
            return None

        want = (str(to_address), int(amount), dedup_key or '')
        what = f'{amount} {self.chain_def.native_unit} to {to_address}'
        decided, result = self.chain.resolve_prior_send(want, what)
        if decided:
            return result

        # Solvency before broadcast: the token leg AND the SOL that carries it (fee, plus rent for a
        # destination with no ATA yet) — a token-rich, SOL-poor signer must refuse, not fail on-chain.
        try:
            mint = self.mint
            held = self.get_balance(str(me))
            if held < int(amount):
                bt.logging.error(f'{LOG_SOL} {self.chain_def.id} balance {held} < {amount} — not sending')
                return None
            lamports_needed = TX_FEE_LAMPORTS + (0 if self._token_account(to_address) else ATA_RENT_LAMPORTS)
            sol = int(self.rpc.get_balance(str(me)))
            if sol < lamports_needed:
                bt.logging.error(
                    f'{LOG_SOL} signer holds {sol} lamports < {lamports_needed} needed for fee+rent — not sending'
                )
                return None
            dst_owner = Pubkey.from_string(to_address)
            ixs = [
                create_ata_idempotent_ix(me, dst_owner, mint),
                transfer_checked_ix(
                    self.ata(str(me)), mint, self.ata(to_address), me, int(amount), self.chain_def.decimals
                ),
            ]
        except Exception as e:
            bt.logging.error(f'{LOG_SOL} {self.chain_def.id} send_amount build failed: {e}')
            return None
        return self.chain.broadcast(ixs, want, what)

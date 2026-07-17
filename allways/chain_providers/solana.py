import base64
import time
from typing import Any, List, Optional, Tuple

import bittensor as bt
import requests
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature

from allways.chain_providers.base import ChainProvider, ProviderUnreachableError, TransactionInfo
from allways.chains import CHAIN_SOL, ChainDefinition
from allways.solana.rpc import SolanaRpc, resolve_rpc_url

LOG_SOL = '[Solana]'


class SolanaProvider(ChainProvider):
    """Solana swap-leg provider for native SOL transfers.

    The SOL leg of a launch pair (sol↔btc, sol↔tao) is a peer-to-peer user↔miner
    transfer — the swap-manager program never custodies it. It's verified like a BTC
    deposit: look the signature up by hash via getTransaction and confirm a >= lamport
    credit to the expected address (balance-delta on pre/postBalances, robust to how the
    transfer was issued).

    Reuses ``allways/solana/rpc.py``. The native-SOL path is isolated in
    ``_match_native_credit`` and ``send_amount`` so an SPL-token leg (sol↔usdc later) drops
    in as a sibling branch — diffing meta.pre/postTokenBalances and building a token
    transfer — without touching this interface.

    Read path needs only an RPC URL; ``send_amount`` (the miner's dest leg) needs a keypair.
    """

    def __init__(
        self, solana_rpc_url: Optional[str] = None, solana_keypair: Optional[Keypair] = None, timeout: int = 30
    ):
        self.rpc_url = resolve_rpc_url(solana_rpc_url)
        self.rpc = SolanaRpc(self.rpc_url, timeout=timeout)
        self.keypair = solana_keypair

    def get_chain(self) -> ChainDefinition:
        return CHAIN_SOL

    def describe(self) -> str:
        return f'Solana RPC {self.rpc_url}'

    def can_send_from(self, address: str) -> bool:
        return self.keypair is not None and str(self.keypair.pubkey()) == address

    def check_connection(self, require_send: bool = True, **kwargs) -> None:
        if require_send and self.keypair is None:
            raise ConnectionError('SOL send requires a Solana keypair (pass solana_keypair)')
        try:
            slot = self.rpc.get_slot()
            bt.logging.success(f'{LOG_SOL} connected: slot={slot}')
        except Exception as e:
            raise ConnectionError(f'Cannot reach Solana RPC at {self.rpc_url}: {e}') from e

    def fetch_matching_tx(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,  # unused — Solana indexes by signature
        max_scan_blocks: int = 150,  # unused — native by-signature lookup, no scan
    ) -> Optional[TransactionInfo]:
        """Look up a SOL transfer by its signature (``tx_hash``) and match the credit.

        Solana resolves transactions by signature natively, so — like Bitcoin's
        getrawtransaction — this is an O(1) lookup, not a block scan. Raises
        ProviderUnreachableError on transient RPC failure.
        """
        if not tx_hash:
            return None
        try:
            tx = self.rpc.get_transaction(tx_hash)
        except (requests.ConnectionError, requests.Timeout) as e:
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

        keys = self._account_keys(tx, meta)
        credit = self._match_native_credit(keys, meta, expected_recipient)
        if credit is None or credit < expected_amount:
            bt.logging.warning(
                f'{LOG_SOL} tx {tx_hash[:16]}... credits {expected_recipient} {credit} lamports '
                f'(< {expected_amount} required)'
            )
            return None

        slot = tx.get('slot')
        block_time = tx.get('blockTime')  # unix seconds, the replay-freshness floor (B2)
        confirmations = self._confirmations(slot)
        sender = keys[0] if keys else ''  # fee payer / first signer == the transfer source
        return TransactionInfo(
            tx_hash=tx_hash,
            confirmed=confirmations >= self.get_chain().min_confirmations,
            sender=sender,
            recipient=expected_recipient,
            amount=credit,
            block_number=slot,
            confirmations=confirmations,
            block_time=block_time,
        )

    @staticmethod
    def _account_keys(tx: dict, meta: dict) -> List[str]:
        """Full account list: the message's static keys plus any address-lookup-table
        loaded addresses (writable then readonly), to index pre/postBalances correctly."""
        msg = (tx.get('transaction') or {}).get('message') or {}
        raw = msg.get('accountKeys') or []
        keys = [k if isinstance(k, str) else (k or {}).get('pubkey') for k in raw]
        loaded = meta.get('loadedAddresses') or {}
        keys += list(loaded.get('writable') or [])
        keys += list(loaded.get('readonly') or [])
        return keys

    @staticmethod
    def _match_native_credit(keys: List[str], meta: dict, recipient: str) -> Optional[int]:
        """Net lamports credited to ``recipient`` in this tx (postBalance − preBalance),
        or None if the address isn't in the tx. Balance-delta is robust to how the transfer
        was issued (plain transfer, CPI, batched). The SPL-token leg will instead diff
        meta.pre/postTokenBalances for the recipient's token account."""
        pre = meta.get('preBalances') or []
        post = meta.get('postBalances') or []
        if recipient not in keys:
            return None
        i = keys.index(recipient)
        if i >= len(pre) or i >= len(post):
            return None
        return int(post[i]) - int(pre[i])

    def _confirmations(self, slot: Optional[int]) -> int:
        """Confirmations = slots since the tx's slot (the tx slot counts as 1). 0 if unknown.

        Reads the pass-cached tip so N legs in one forward pass share a single ``getSlot`` instead
        of one each — the getSlot count drops from per-leg to per-pass."""
        if slot is None:
            return 0
        tip = self.cached_block_height()
        if tip is None:
            return 0
        return max(0, tip - int(slot) + 1)

    def get_current_block_height(self) -> Optional[int]:
        """Current confirmed slot. None on transient backend failure."""
        try:
            return int(self.rpc.get_slot())
        except Exception as e:
            bt.logging.debug(f'SOL get_current_block_height failed: {e}')
            return None

    def get_balance(self, address: str) -> int:
        """Account balance in lamports (0 for a never-funded address)."""
        try:
            return int(self.rpc.get_balance(address))
        except Exception as e:
            bt.logging.error(f'SOL get_balance failed for {address}: {e}')
            return 0

    def is_valid_address(self, address: str) -> bool:
        """Validate a base58 ed25519 pubkey (32 bytes) without RPC."""
        if not address or not isinstance(address, str):
            return False
        try:
            Pubkey.from_string(address)
            return True
        except Exception:
            return False

    def sign_from_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        """Sign a proof message with an ed25519 Solana keypair. Returns hex signature.

        ``key`` may be a solders Keypair; falls back to the provider's own keypair."""
        kp = key if isinstance(key, Keypair) else self.keypair
        if kp is None:
            bt.logging.error('No Solana keypair available for signing')
            return ''
        try:
            return bytes(kp.sign_message(message.encode())).hex()
        except Exception as e:
            bt.logging.error(f'{LOG_SOL} sign_from_proof failed: {e}')
            return ''

    def verify_from_proof(self, address: str, message: str, signature: str) -> bool:
        """Verify an ed25519 signature over ``message`` from the given base58 pubkey."""
        try:
            pubkey = Pubkey.from_string(address)
            sig_hex = signature[2:] if signature.startswith('0x') else signature
            sig = Signature.from_bytes(bytes.fromhex(sig_hex))
            return sig.verify(pubkey, message.encode())
        except Exception as e:
            bt.logging.error(f'{LOG_SOL} verify_from_proof failed: {e}')
            return False

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None
    ) -> Optional[Tuple[str, int]]:
        """Send ``amount`` lamports to ``to_address`` via SystemProgram transfer.

        Signs with the provider's own keypair (callers pass no key material). Returns
        (signature, slot) or None. Retries on stale-blockhash like the program client."""
        if self.keypair is None:
            bt.logging.error('SOL send_amount called on a read-only SolanaProvider (no keypair)')
            return None
        try:
            from solders.hash import Hash
            from solders.system_program import TransferParams, transfer
            from solders.transaction import Transaction

            ix = transfer(
                TransferParams(
                    from_pubkey=self.keypair.pubkey(), to_pubkey=Pubkey.from_string(to_address), lamports=int(amount)
                )
            )
        except Exception as e:
            bt.logging.error(f'{LOG_SOL} send_amount build failed: {e}')
            return None

        last_err: Optional[Exception] = None
        for _ in range(5):
            try:
                blockhash = Hash.from_string(self.rpc.get_latest_blockhash())
                tx = Transaction.new_signed_with_payer([ix], self.keypair.pubkey(), [self.keypair], blockhash)
                sig = self.rpc.send_transaction(base64.b64encode(bytes(tx)).decode())
                status = self.rpc.confirm(sig)
                slot = int(status.get('slot') or 0)
                bt.logging.info(f'{LOG_SOL} sent {amount} lamports to {to_address} (sig: {sig}, slot: {slot})')
                return (sig, slot)
            except Exception as e:  # transient stale-blockhash on a fresh/lagging RPC → re-fetch + resend
                if 'blockhash' not in str(e).lower():
                    bt.logging.error(f'{LOG_SOL} send_amount failed: {e}')
                    return None
                last_err = e
                time.sleep(0.5)
        bt.logging.error(f'{LOG_SOL} send_amount failed after blockhash retries: {last_err}')
        return None

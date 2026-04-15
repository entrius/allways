from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Tuple

import bittensor as bt

from allways.chains import ChainDefinition


class ProviderUnreachableError(Exception):
    """Raised when a chain provider cannot reach its backend during verification."""


@dataclass
class TransactionInfo:
    tx_hash: str
    confirmed: bool
    sender: str
    recipient: str
    amount: int  # Smallest unit (satoshis / rao)
    block_number: Optional[int] = None
    confirmations: int = 0


class ChainProvider(ABC):
    """Abstract interface for chain verification.

    Adding a new chain:
    1. Add ChainDefinition to chains.py
    2. Implement this interface
    3. Add {ENV_PREFIX}_* vars to .env
    """

    @abstractmethod
    def get_chain(self) -> ChainDefinition: ...

    @abstractmethod
    def check_connection(self, **kwargs) -> None:
        """Verify the chain provider can reach its backend (RPC node, subtensor, etc).

        Raises ConnectionError with a descriptive message on failure.
        Called during startup to fail fast if a provider is misconfigured.
        """
        ...

    @abstractmethod
    def _fetch_matching_tx(
        self, tx_hash: str, expected_recipient: str, expected_amount: int, block_hint: int = 0
    ) -> Optional[TransactionInfo]:
        """Chain-specific fetch — return TransactionInfo if the tx exists and matches
        recipient + amount, otherwise None. Raises ProviderUnreachableError on
        transient backend failures.

        Uses >= for amount (overpayment is acceptable on-chain).
        block_hint: If > 0, providers can use this for O(1) lookup instead of scanning.

        Not called directly by application code — use ``verify_transaction``,
        which wraps this with the common confirmed/sender post-checks.
        """
        ...

    def verify_transaction(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        expected_sender: Optional[str] = None,
        require_confirmed: bool = False,
    ) -> Optional[TransactionInfo]:
        """Verify a transaction against the shared post-fetch checklist.

        Dispatches to the provider's ``_fetch_matching_tx`` for the chain-specific
        scan, then applies the common checks every caller cares about:

        - ``require_confirmed`` — if True, reject txs that don't have enough
          confirmations for the chain. Default False, because axon/pending-confirm
          flows want the partial TransactionInfo so they can queue and retry.
        - ``expected_sender`` — if provided, reject txs whose sender doesn't match.
          Tolerates empty sender from the provider (unparseable vin/extrinsic); the
          stricter ``SwapVerifier`` path keeps its own inline check.

        Rejections are logged once in the base so observability for the defense
        is in one place instead of duplicated at every call site.
        """
        tx_info = self._fetch_matching_tx(
            tx_hash=tx_hash,
            expected_recipient=expected_recipient,
            expected_amount=expected_amount,
            block_hint=block_hint,
        )
        if tx_info is None:
            return None

        if require_confirmed and not tx_info.confirmed:
            bt.logging.debug(
                f'verify_transaction: tx {tx_hash[:16]}... not yet confirmed '
                f'({tx_info.confirmations}/{self.get_chain().min_confirmations})'
            )
            return None

        if expected_sender and tx_info.sender and tx_info.sender != expected_sender:
            bt.logging.warning(
                f'verify_transaction: sender mismatch on tx {tx_hash[:16]}... '
                f'(expected {expected_sender}, got {tx_info.sender})'
            )
            return None

        return tx_info

    @abstractmethod
    def get_balance(self, address: str) -> int: ...

    @abstractmethod
    def is_valid_address(self, address: str) -> bool: ...

    @abstractmethod
    def sign_from_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        """Sign a source proof message with the given key. Returns hex signature."""
        ...

    @abstractmethod
    def verify_from_proof(self, address: str, message: str, signature: str) -> bool:
        """Verify a source proof signature from the given address."""
        ...

    @abstractmethod
    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None
    ) -> Optional[Tuple[str, int]]:
        """Send funds to an address. Returns (tx_hash, block_number) or None.

        Providers own their own signing credentials — TAO uses the ``bt.Wallet``
        passed at construction, BTC reads ``BTC_PRIVATE_KEY`` / RPC wallet from
        env. Callers do not pass key material.

        amount: in smallest unit (satoshis / rao)
        from_address: hint for sender's address type (used by BTC lightweight to
                      derive correct type from WIF)
        """
        ...

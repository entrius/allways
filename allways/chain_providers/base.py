from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Tuple

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
    def verify_transaction(
        self, tx_hash: str, expected_recipient: str, expected_amount: int, block_hint: int = 0
    ) -> Optional[TransactionInfo]:
        """Verify a transaction; returns TransactionInfo if found, None if not found,
        raises ProviderUnreachableError on transient failures.

        Uses >= for amount (overpayment is acceptable on-chain).
        block_hint: If > 0, providers can use this for O(1) lookup instead of scanning.
        """
        ...

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

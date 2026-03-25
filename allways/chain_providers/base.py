from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from allways.chains import ChainDefinition


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
        """Verify a transaction. Uses >= for amount (overpayment is acceptable on-chain).

        block_hint: If > 0, the block number where the tx is expected to be found.
        Providers can use this for O(1) lookup instead of scanning.
        """
        ...

    @abstractmethod
    def get_balance(self, address: str) -> int: ...

    @abstractmethod
    def is_valid_address(self, address: str) -> bool: ...

    @abstractmethod
    def sign_source_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        """Sign a source proof message with the given key. Returns hex signature."""
        ...

    @abstractmethod
    def verify_source_proof(self, address: str, message: str, signature: str) -> bool:
        """Verify a source proof signature from the given address."""
        ...

    @abstractmethod
    def send_amount(
        self, to_address: str, amount: int, key: Optional[Any] = None, from_address: Optional[str] = None
    ) -> Optional[Tuple[str, int]]:
        """Send funds to an address. Returns (tx_hash, block_number) or None.

        amount: in smallest unit (satoshis / rao)
        key: chain-specific signing key (e.g., bt.Wallet for TAO, None for BTC if using RPC wallet)
        from_address: hint for sender's address type (used by BTC lightweight to derive correct type from WIF)
        """
        ...

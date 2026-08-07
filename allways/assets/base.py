from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Tuple

import bittensor as bt

from allways.assets.chain import Chain
from allways.chains import ChainDefinition


class ProviderUnreachableError(Exception):
    """Raised when a chain provider cannot reach its backend during verification."""


# What ``send_amount`` resolves to: (tx_hash, block_number) on success — block_number is 0 when
# the chain reports inclusion asynchronously — or None when the send was refused/failed.
SendResult = Optional[Tuple[str, int]]


@dataclass
class TransactionInfo:
    tx_hash: str
    confirmed: bool  # True once the tx has the chain's min_confirmations
    sender: str  # provable payer (first input / from / Transfer-log party); '' = unprovable
    recipient: str
    amount: int  # Smallest unit (satoshis / rao / wei / µUSDC)
    block_number: Optional[int] = None  # None while in the mempool
    confirmations: int = 0
    block_time: Optional[int] = None  # Block's mined time, unix seconds (replay-freshness floor; B2)


class Asset(ABC):
    """A thing you can swap: verify a payment of it, send it, gate it.

    Every asset lives on a `Chain` (`self.chain`). Single-asset networks (btc/tao/sol,
    and eth until a second EVM asset lands) fuse the two: the class implements both and
    `.chain` returns itself. The registry row stays a `ChainDefinition` in chains.py —
    the wire says chain; in code an id resolves to an Asset.

    Seam rule: asset-side code asks chain questions through ``self.chain``; chain
    members calling each other keep plain ``self`` (they move together on a split).

    Adding a new asset:
    1. Add its ChainDefinition to chains.py
    2. Implement this interface (or reuse a family class, e.g. an ERC-20)
    3. Register it in ASSET_REGISTRY; add {ENV_PREFIX}_* vars to .env
    """

    # Non-fused assets (tokens on a multi-asset chain) set this at construction.
    _chain: Optional[Chain] = None

    @property
    @abstractmethod
    def chain_def(self) -> ChainDefinition:
        """This asset's registry row (chains.py): wire id, decimals, confirmations, env prefix.

        ``chain_def`` is the asset's static registry FACTS (``chains.get_chain_def(id)`` reaches
        the same row from a wire id); ``chain`` is the live network OBJECT it talks through."""

    @property
    def chain(self) -> Chain:
        """The network this asset lives on. Fused single-asset classes are their own chain."""
        if self._chain is not None:
            return self._chain
        if isinstance(self, Chain):
            return self
        raise NotImplementedError(f'{type(self).__name__} must bind a Chain (set self._chain)')

    def describe(self) -> str:
        """One-line summary of the backend/API this provider talks to, for startup logs."""
        return self.chain_def.name

    def clear_cache(self) -> None:
        """Reset per-asset scratch caches at the start of a validator pass. No-op by default."""

    def can_send_from(self, address: str) -> bool:
        """True iff this provider's signing credentials control ``address`` — i.e. a ``send_amount``
        would broadcast FROM it. Gates the CLI's auto-send: a deposit must come from the reservation's
        pinned ``from_addr`` or the validator rejects it. Conservative default (unknown provider can't
        prove it); each provider overrides with a real check."""
        return False

    @abstractmethod
    def check_connection(self, **kwargs) -> None:
        """Verify the chain provider can reach its backend (RPC node, subtensor, etc).

        Raises ConnectionError with a descriptive message on failure.
        Called during startup to fail fast if a provider is misconfigured.
        """
        ...

    @abstractmethod
    def fetch_matching_tx(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        max_scan_blocks: int = 150,
    ) -> Optional[TransactionInfo]:
        """Chain-specific fetch — return TransactionInfo if the tx exists and matches
        recipient + amount, otherwise None. Raises ProviderUnreachableError on
        transient backend failures.

        Uses >= for amount (overpayment is acceptable on-chain).
        block_hint: If > 0, providers can use this for O(1) lookup instead of scanning.
        max_scan_blocks: Upper bound on how far back to scan when ``block_hint``
        is 0. CLI lookups can pass a wider window (e.g. reservation lifetime) so
        users who post-tx late still resolve the block client-side. Ignored by
        providers with a native "find tx by hash" RPC (e.g. Bitcoin).

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
        max_scan_blocks: int = 150,
    ) -> Optional[TransactionInfo]:
        """Verify a transaction against the shared post-fetch checklist.

        Dispatches to the provider's ``fetch_matching_tx`` for the chain-specific
        scan, then applies the common checks every caller cares about:

        - ``require_confirmed`` — if True, reject txs that don't have enough
          confirmations for the chain. Default False, because axon/pending-confirm
          flows want the partial TransactionInfo so they can queue and retry.
        - ``expected_sender`` — if provided, reject txs whose sender doesn't
          match. Strict: an empty/unparseable sender from the provider also
          fails the check, since we can't prove the tx came from the reserved
          address. Closing this gap prevents a "malformed-input evades the
          defense" class of attack.

        Rejections are logged once in the base so observability for the defense
        is in one place instead of duplicated at every call site.
        """
        # Canonicalize expecteds once here so every fetcher compares in canonical form.
        chain = self.chain
        expected_recipient = chain.normalize_address(expected_recipient)
        expected_sender = chain.normalize_address(expected_sender) if expected_sender else expected_sender

        tx_info = self.fetch_matching_tx(
            tx_hash=tx_hash,
            expected_recipient=expected_recipient,
            expected_amount=expected_amount,
            block_hint=block_hint,
            max_scan_blocks=max_scan_blocks,
        )
        if tx_info is None:
            return None

        if require_confirmed and not tx_info.confirmed:
            bt.logging.debug(
                f'verify_transaction: tx {tx_hash[:16]}... not yet confirmed '
                f'({tx_info.confirmations}/{self.chain_def.min_confirmations})'
            )
            return None

        if expected_sender and chain.normalize_address(tx_info.sender) != expected_sender:
            bt.logging.warning(
                f'verify_transaction: sender mismatch on tx {tx_hash[:16]}... '
                f'(expected {expected_sender}, got {tx_info.sender!r})'
            )
            return None

        # A self-transfer (sender == recipient) is never a real swap leg: the
        # paying and receiving parties are always distinct. Rejecting it blocks
        # same-wallet A->A volume fakes; A->B self-flow is bounded economically.
        if tx_info.sender and chain.normalize_address(tx_info.sender) == expected_recipient:
            bt.logging.warning(
                f'verify_transaction: self-transfer on tx {tx_hash[:16]}... '
                f'(sender == recipient {expected_recipient}) — rejecting'
            )
            return None

        return tx_info

    @abstractmethod
    def get_balance(self, address: str) -> int: ...

    def can_deliver_to(self, address: str, amount: int) -> bool:
        """Reserve-time gate: False only on positive evidence the destination cannot receive."""
        return True

    def delivery_refused(self, address: str, since_unix: int) -> bool:
        """Slash gate: True only on positive evidence delivery was refusable since ``since_unix``."""
        return False

    @abstractmethod
    def send_amount(self, to_address: str, amount: int, from_address: Optional[str] = None) -> SendResult:
        """Send funds to an address. Returns (tx_hash, block_number) or None.

        Providers own their own signing credentials — TAO uses the ``bt.Wallet``
        passed at construction, BTC reads ``BTC_PRIVATE_KEY`` / RPC wallet from
        env. Callers do not pass key material.

        amount: in smallest unit (satoshis / rao)
        from_address: hint for sender's address type (used by BTC lightweight to
                      derive correct type from WIF)
        """
        ...

import re
import time
from abc import ABC, abstractmethod
from typing import Any, Optional

_HEX64 = re.compile(r'[0-9a-fA-F]{64}')


def canonical_hex_tx_hash(tx_hash: str, prefixed: bool) -> Optional[str]:
    """Lowercase 32-byte hex hash, ``0x``-prefixed when ``prefixed``; None when it is anything else."""
    if not isinstance(tx_hash, str):
        return None
    if prefixed:
        if tx_hash[:2] not in ('0x', '0X'):
            return None
        tx_hash = tx_hash[2:]
    if not _HEX64.fullmatch(tx_hash):
        return None
    return ('0x' if prefixed else '') + tx_hash.lower()


class Chain(ABC):
    """A network you can talk to: reach it, read blocks/txs, know its address and proof rules.

    Assets live on chains (`Asset.chain`). Single-asset networks fuse the two — the
    asset class also implements Chain and `.chain` returns itself — and split the day
    the chain hosts its second asset. Multi-asset families (EVM) bind a shared,
    config-driven Chain instance per network instead of a class per chain.

    Everything here is a fact of the network, shared by every asset on it: the tip,
    the address format, the ownership-proof signature scheme. Transport (sessions,
    RPC ladders) moves here when the first split lands.
    """

    # A cached tip is reused for at most this long. The validator clears it each forward pass (one
    # getSlot/pass); this TTL is the safety net for callers that never clear (e.g. the miner), so the
    # tip can never freeze — it self-refreshes at least this often. Slightly longer than a subtensor
    # block so a validator pass stays on one fetch.
    _TIP_TTL_SECONDS = 15.0
    _pass_tip: Optional[int] = None
    _pass_tip_ts: float = 0.0

    @abstractmethod
    def get_current_block_height(self) -> Optional[int]:
        """Chain tip block height. None on transient backend failure."""
        ...

    def cached_block_height(self) -> Optional[int]:
        """Chain tip, cached so per-tx confirmation math shares one lookup instead of one per leg.

        The validator clears this each pass (``clear_pass_tip``) → one ``getSlot`` per pass. The TTL
        caps staleness for callers that never clear (the miner), so the tip can never freeze. A start-
        of-pass/slightly-stale tip biases confirmations low — conservative, never a false confirm. A
        failed fetch (None) is not cached, so it retries."""
        if self._pass_tip is not None and time.monotonic() - self._pass_tip_ts < self._TIP_TTL_SECONDS:
            return self._pass_tip
        tip = self.get_current_block_height()
        if tip is not None:
            self._pass_tip = tip
            self._pass_tip_ts = time.monotonic()
        return tip

    def clear_pass_tip(self) -> None:
        """Expire the cached tip — the validator calls this once per pass for a fresh start-of-pass tip."""
        self._pass_tip = None

    @abstractmethod
    def is_valid_address(self, address: str) -> bool: ...

    def normalize_address(self, address: str) -> str:
        """Canonical comparison form for this chain's addresses (identity by default).

        Chains whose address encoding is case-insensitive (ETH hex / EIP-55 checksum casing)
        override this so equality checks never fail on casing alone. SOURCE addresses commit
        on-chain in this form (the source-lock PDA is byte-keyed on it — V-C2); delivery
        addresses stay as given (EVM sends re-checksum at signing)."""
        return address

    def canonical_tx_hash(self, tx_hash: str) -> Optional[str]:
        """The one spelling of a tx hash, or None when malformed. A Swap PDA is keyed by the raw hash
        string, so chains whose RPCs accept several spellings must collapse them here (identity by default)."""
        return tx_hash or None

    @abstractmethod
    def sign_from_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        """Sign a source proof message with the given key. Returns hex signature."""
        ...

    @abstractmethod
    def verify_from_proof(self, address: str, message: str, signature: str) -> bool:
        """Verify a source proof signature from the given address."""
        ...

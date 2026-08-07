import time
from abc import ABC, abstractmethod
from typing import Any, Optional


class Chain(ABC):
    """A network you can talk to: reach it, read blocks/txs, know its address and proof rules.

    Assets live on chains (`Asset.chain`). Single-asset networks fuse the two — the
    asset class also implements Chain and `.chain` returns itself — and split the day
    the chain hosts its second asset. Multi-asset families (EVM) bind a shared,
    config-driven Chain instance per network instead of a class per chain.

    Everything here is a fact of the network, shared by every asset on it: the tip,
    the address format, the ownership-proof signature scheme, the transport.
    """

    # A cached tip is reused for at most this long. The validator clears it each forward pass (one
    # getSlot/pass); this TTL is the safety net for callers that never clear (e.g. the miner), so the
    # tip can never freeze — it self-refreshes at least this often. Slightly longer than a subtensor
    # block so a validator pass stays on one fetch.
    _TIP_TTL_SECONDS = 15.0

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
        tip = getattr(self, '_pass_tip', None)
        if tip is not None and time.monotonic() - getattr(self, '_pass_tip_ts', 0.0) < self._TIP_TTL_SECONDS:
            return tip
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
        override this so equality checks never fail on casing alone. Comparison only — never
        feed the normalized form back on-chain or into display."""
        return address

    @abstractmethod
    def sign_from_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        """Sign a source proof message with the given key. Returns hex signature."""
        ...

    @abstractmethod
    def verify_from_proof(self, address: str, message: str, signature: str) -> bool:
        """Verify a source proof signature from the given address."""
        ...

"""sr25519 hotkey-binding verification + pubkey→hotkey attribution (B3).

On-chain (A5) a miner stores its Bittensor hotkey + an sr25519 signature *by the hotkey, over the miner's
Solana pubkey* on the `Binding` PDA; the contract only stores it (sr25519 verify is too costly on-chain).
The validator verifies it here so on-chain state keyed by Solana pubkey (MinerState counters,
MinerDirectionStats, events) attributes to the right Bittensor hotkey/UID. The contract already enforces
hotkey→≤1 pubkey (set-once `HotkeyBinding`); we mirror that off-chain (first-bound wins) as defense in depth
so a struck pubkey can't rotate to dodge strikes.
"""

from typing import Dict, Optional

import bittensor as bt
from bittensor import Keypair


def _hotkey_keypair(hotkey_bytes: bytes) -> Keypair:
    # bittensor's Keypair wants public_key as a 0x-hex string, not raw bytes.
    return Keypair(public_key='0x' + bytes(hotkey_bytes).hex())


def verify_binding(miner_pubkey, hotkey_bytes: bytes, hotkey_sig: bytes) -> bool:
    """True iff `hotkey_bytes` (sr25519 pubkey) signed the miner's Solana pubkey bytes (A5 mutual auth)."""
    try:
        return bool(_hotkey_keypair(hotkey_bytes).verify(bytes(miner_pubkey), bytes(hotkey_sig)))
    except Exception as e:
        bt.logging.debug(f'binding verify error: {e}')
        return False


def hotkey_ss58(hotkey_bytes: bytes) -> Optional[str]:
    """sr25519 pubkey bytes → ss58 address (for metagraph UID lookup)."""
    try:
        return _hotkey_keypair(hotkey_bytes).ss58_address
    except Exception:
        return None


def warn_if_unbound(solana_client) -> None:
    """Startup nudge: peers can only derive this validator's stake weight if its pubkey carries a
    hotkey binding. Best-effort — a read failure never blocks startup."""
    try:
        unbound = solana_client.get_binding(solana_client.keypair.pubkey()) is None
    except Exception as e:
        bt.logging.debug(f'binding self-check skipped: {e}')
        return
    if unbound:
        bt.logging.warning(
            'solana pubkey has no hotkey binding — run `alw bind-hotkey` so peers can post your stake weight'
        )


def build_attribution(solana_client) -> Dict[str, str]:
    """Map miner Solana pubkey (str) → bound hotkey ss58 for every valid binding.

    Each `Binding` PDA is per-miner, so pubkey→hotkey is inherently 1:1. Hotkey collisions (which the
    contract's set-once marker prevents) are resolved first-bound-wins by `bound_at` for determinism.
    """
    bindings = []
    for _pda, b in solana_client.get_all('Binding'):
        bindings.append(b)
    # Deterministic first-seen: earliest bound_at wins a contested hotkey (pubkey str tiebreak).
    bindings.sort(key=lambda b: (int(b.bound_at), str(b.miner)))

    pubkey_to_hotkey: Dict[str, str] = {}
    hotkey_owner: Dict[str, str] = {}  # ss58 → pubkey str (first binder)
    for b in bindings:
        miner = str(b.miner)
        if not verify_binding(b.miner, b.hotkey, b.hotkey_sig):
            bt.logging.warning(f'binding for {miner[:8]}: invalid sr25519 sig, skipping')
            continue
        ss58 = hotkey_ss58(b.hotkey)
        if ss58 is None:
            continue
        owner = hotkey_owner.get(ss58)
        if owner is not None and owner != miner:
            bt.logging.warning(f'hotkey {ss58[:8]} already bound to {owner[:8]}; rejecting {miner[:8]}')
            continue
        hotkey_owner[ss58] = miner
        pubkey_to_hotkey[miner] = ss58
    return pubkey_to_hotkey

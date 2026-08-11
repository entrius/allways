"""PDA derivation for the allways_swap_manager program.

Seeds mirror smart-contracts/solana/.../constants.rs. Composite seeds:
  quote         : [b"quote", miner, from_chain, to_chain, collateral_chain]
  stats         : [b"stats", miner, from_chain, to_chain]
  vote          : [b"vote", [req_type], target]  (weights round: target = the 32-byte snapshot hash)
  swap          : [b"swap", swap_key]   (swap_key = keccak(from_tx_hash), 32 bytes)
  hkbind        : [b"hkbind", hotkey]   (hotkey = 32-byte sr25519 pubkey)
  attest        : [b"attest", miner, chain_id]
  attest round  : [b"vote", [REQ_SET_ATTESTATION], miner, chain_id]  (composite target)
"""

from typing import Optional

from solders.pubkey import Pubkey

from allways.solana.program import resolve_program_id

# Vote-round request types (constants.rs). REQ_RESERVE is gone (lottery-based).
REQ_ACTIVATE = 0
REQ_INITIATE = 2
REQ_DEACTIVATE = 5
REQ_CONFIRM = 6
REQ_TIMEOUT = 7
REQ_SET_WEIGHTS = 8
REQ_SET_ATTESTATION = 9
REQ_ATTEST_HEARTBEAT = 10

# Per-backing activation bits on MinerState.active_backings (constants.rs).
BACKING_BIT_SOL = 1 << 0
BACKING_BIT_TAO = 1 << 1

# Backing (collateral) chain ids and their bits — the registry `backing.rs` matches on.
BACKING_CHAIN_SOL = 'sol'
BACKING_CHAIN_TAO = 'tao'
BACKING_BITS = {BACKING_CHAIN_SOL: BACKING_BIT_SOL, BACKING_CHAIN_TAO: BACKING_BIT_TAO}


def _pk_bytes(p) -> bytes:
    """Accept a solders Pubkey or raw 32 bytes/str → 32-byte seed."""
    if isinstance(p, Pubkey):
        return bytes(p)
    if isinstance(p, (bytes, bytearray)):
        return bytes(p)
    return bytes(Pubkey.from_string(str(p)))


def _derive(seeds, program_id: Optional[Pubkey] = None) -> Pubkey:
    return Pubkey.find_program_address(seeds, program_id or resolve_program_id())[0]


def config_pda(program_id: Optional[Pubkey] = None) -> Pubkey:
    return _derive([b'config'], program_id)


def treasury_pda(program_id: Optional[Pubkey] = None) -> Pubkey:
    return _derive([b'treasury'], program_id)


def miner_state_pda(miner, program_id: Optional[Pubkey] = None) -> Pubkey:
    return _derive([b'miner', _pk_bytes(miner)], program_id)


def collateral_vault_pda(miner, program_id: Optional[Pubkey] = None) -> Pubkey:
    return _derive([b'collateral', _pk_bytes(miner)], program_id)


def binding_pda(miner, program_id: Optional[Pubkey] = None) -> Pubkey:
    return _derive([b'bind', _pk_bytes(miner)], program_id)


def hotkey_binding_pda(hotkey: bytes, program_id: Optional[Pubkey] = None) -> Pubkey:
    return _derive([b'hkbind', bytes(hotkey)], program_id)


def bond_attestation_pda(miner, chain: str, program_id: Optional[Pubkey] = None) -> Pubkey:
    return _derive([b'attest', _pk_bytes(miner), chain.encode()], program_id)


def attestation_round_pda(miner, chain: str, program_id: Optional[Pubkey] = None) -> Pubkey:
    """The reusable per-(miner, backing chain) attestation round — a composite target, so it can't go
    through `vote_round_pda` (which takes a single 32-byte one)."""
    return _derive([b'vote', bytes([REQ_SET_ATTESTATION]), _pk_bytes(miner), chain.encode()], program_id)


def reservation_pda(miner, backing: str = BACKING_CHAIN_SOL, program_id: Optional[Pubkey] = None) -> Pubkey:
    """One reservation slot per (miner, hub) — the backing is in the seeds (v3.1)."""
    return _derive([b'resv', _pk_bytes(miner), backing.encode()], program_id)


def pool_pda(miner, backing: str = BACKING_CHAIN_SOL, program_id: Optional[Pubkey] = None) -> Pubkey:
    """One lottery-contest slot per (miner, hub) — the backing is in the seeds (v3.1)."""
    return _derive([b'pool', _pk_bytes(miner), backing.encode()], program_id)


def legacy_reservation_pda(miner, program_id: Optional[Pubkey] = None) -> Pubkey:
    """The RETIRED pre-v3.1 per-miner address — only `close_legacy_reservation` resolves it now."""
    return _derive([b'resv', _pk_bytes(miner)], program_id)


def legacy_pool_pda(miner, program_id: Optional[Pubkey] = None) -> Pubkey:
    """The RETIRED pre-v3.1 per-miner address — only `close_legacy_pool` resolves it now."""
    return _derive([b'pool', _pk_bytes(miner)], program_id)


def legacy_initiate_round_pda(miner, program_id: Optional[Pubkey] = None) -> Pubkey:
    """The RETIRED per-miner initiate round (live rounds key by swap_key) — closer-only."""
    return _derive([b'vote', bytes([REQ_INITIATE]), _pk_bytes(miner)], program_id)


def swap_pda(swap_key: bytes, program_id: Optional[Pubkey] = None) -> Pubkey:
    return _derive([b'swap', bytes(swap_key)], program_id)


def quote_pda(
    miner,
    from_chain: str,
    to_chain: str,
    backing: str = BACKING_CHAIN_SOL,
    program_id: Optional[Pubkey] = None,
) -> Pubkey:
    """One quote per (miner, direction, BACKING) — the backing is in the seeds so a dual-purse miner
    can stand two offers on the same hub<->hub direction at different rates (W2b/D2)."""
    return _derive(
        [b'quote', _pk_bytes(miner), from_chain.encode(), to_chain.encode(), backing.encode()],
        program_id,
    )


def legacy_quote_pda(miner, from_chain: str, to_chain: str, program_id: Optional[Pubkey] = None) -> Pubkey:
    """The pre-W2b four-seed derivation. Only `close_legacy_quote` still needs it — every quote written
    at this address was orphaned by the seed change and holds rent nobody can otherwise reclaim."""
    return _derive([b'quote', _pk_bytes(miner), from_chain.encode(), to_chain.encode()], program_id)


def stats_pda(miner, from_chain: str, to_chain: str, program_id: Optional[Pubkey] = None) -> Pubkey:
    return _derive([b'stats', _pk_bytes(miner), from_chain.encode(), to_chain.encode()], program_id)


def vote_round_pda(req_type: int, target=None, program_id: Optional[Pubkey] = None) -> Pubkey:
    """Per-target vote round. REQ_SET_WEIGHTS rounds are keyed per snapshot: target = weights_round_key."""
    seeds = [b'vote', bytes([req_type])]
    if target is not None:
        seeds.append(_pk_bytes(target))
    return _derive(seeds, program_id)

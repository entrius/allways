"""Job 3 — the exit sequence and the fee true-up cadence (Solana → vault).

**Exit.** Deactivating the TAO purse on Solana comes FIRST and is one-way until re-lock, which is
what turns "no in-flight swaps" from a snapshot that races the next swap into a stable fact. Only
then does the relayer wait out full quiescence — no live swap, both timeout/settlement windows
past, every slash verdict for this miner applied on the vault — settle the residual fee as a
ONE-ENTRY batch, and finally ``vote_unlock`` at the CURRENT epoch. A miner must never unlock while
fee-encumbered, and the epoch binding means a stale round can't unlock a re-locked bond.

The arming half is persisted, deliberately: on chain a miner that deactivated and one that has
never activated are the same state (purse bit down, bond locked), so inferring an exit from the bit
alone would unlock the bond of a miner that was only waiting to enter service. Everything after
arming is re-derived every pass, so a restart mid-exit simply resumes.

**True-up.** Per-swap fee rounds are uneconomic, so fees ride the attestation off-chain and land
on the vault as one block-boundary-aligned global batch. Every validator fires at the same
boundary, reads totals AT that boundary, and sorts by AccountId — the vector is byte-identical by
construction, which is what lets the round's contents-hash key converge. A boundary whose snapshot
carries no delta is skipped: a no-op round is pure postage.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import bittensor as bt

from allways import dev_signal
from allways.solana.layouts import lock_max
from allways.vault import codec

# Mirrors the vault's MAX_BATCH. Chunking is deterministic (sorted, fixed size), so a fleet that
# needs more than one chunk still produces identical vectors.
MAX_BATCH = 256

_CADENCE_KEY = 'fee_cadence_boundary'


# ─── exit sequence ───────────────────────────────────────────────────────────


def run_exits(relay, now: int) -> bool:
    """Advance every armed exit by at most one step. Returns True when nothing was left owed."""
    ok = True
    for miner in sorted(relay._exiting):
        try:
            if not _advance(relay, miner, now):
                ok = False
        except Exception as e:
            bt.logging.warning(f'relay: exit for {miner[:8]} failed: {e}')
            ok = False
    return ok


def _advance(relay, miner: str, now: int) -> bool:
    hotkey = relay.hotkey_for(miner)
    if hotkey is None:
        return False
    lock = relay.vault.get_lock_state(hotkey)
    if lock is None:
        return False
    locked, epoch = lock
    if not locked:
        relay.disarm_exit(miner)  # nothing left to do — the bond is already free to withdraw
        return True

    quiescent, why = _quiescence(relay, miner, now)
    if not quiescent:
        bt.logging.debug(f'relay: {miner[:8]} exit waiting — {why}')
        return True  # waiting is a healthy state, not an outstanding obligation

    generation = relay.vault_generation()
    if generation is None:
        return False
    total = relay.store.accrued_fee_total(miner, relay.backing, vault_generation=generation)
    settled = relay.vault.get_settled_total(hotkey)
    if settled is None:
        return False
    if total > settled:
        return _settle_residual(relay, miner, hotkey, total, now)
    return _unlock(relay, miner, hotkey, epoch, now)


def _quiescence(relay, miner: str, now: int) -> Tuple[bool, str]:
    """Full quiescence per the active-lock protocol. The permanent swap_ref markers ARE the slash
    checklist — an unapplied verdict means the vault still owes a seizure against this bond."""
    ms = relay.solana.get_miner_state(miner)
    if ms is None:
        return False, 'miner state unreadable'
    if int(getattr(ms, 'active_backings', 0)) & relay.backing_bit:
        relay.disarm_exit(miner)
        return False, 'purse re-activated'
    if getattr(ms, 'has_active_swap', False):
        return False, 'swap still in flight'
    grace = relay.cfg.quiescence_grace_secs
    for field in ('busy_until', 'settling_until'):
        until = lock_max(getattr(ms, field, 0))
        if now < until + grace:
            return False, f'{field} not past (+{until + grace - now}s)'
    if relay.store.open_relay_slashes(relay.backing, miner):
        return False, 'slash verdict not yet applied on the vault'
    return True, ''


def _settle_residual(relay, miner: str, hotkey: str, total: int, now: int) -> bool:
    """The exit residual — at most one cadence of fees, often zero. A ONE-ENTRY batch through the
    same instruction, so an exit never drags the fleet into its round."""
    key = f'settle:{miner}:{total}'
    if relay.throttled(key, now):
        return True
    if not relay.can_write():
        return False
    relay.note_write()
    if relay.read_only:
        bt.logging.info(f'relay: WOULD settle residual {total} for {hotkey[:8]} (read-only)')
        relay.note_vote(key, now)
        return True
    result = relay.vault.vote_collect_fees_batch([(hotkey, total)])
    relay.note_vote(key, now)
    if not result.ok:
        bt.logging.warning(f'relay: residual settle rejected for {hotkey[:8]} ({result.error or "reverted"})')
        return False
    bt.logging.info(f'relay: residual fee settle voted for {hotkey[:8]} (cumulative {total})')
    dev_signal.emit('relay_residual_settle', miner=miner, hotkey=hotkey, cumulative=total)
    return True  # unlock waits for the next pass, once the vault confirms the settle applied


def _unlock(relay, miner: str, hotkey: str, epoch: int, now: int) -> bool:
    key = f'unlock:{miner}:{epoch}'
    if relay.throttled(key, now):
        return True
    if not relay.can_write():
        return False
    relay.note_write()
    if relay.read_only:
        bt.logging.info(f'relay: WOULD vote_unlock {hotkey[:8]} at epoch {epoch} (read-only)')
        relay.note_vote(key, now)
        return True
    result = relay.vault.vote_unlock(hotkey, epoch)
    relay.note_vote(key, now)
    if not result.ok:
        bt.logging.warning(f'relay: vote_unlock rejected for {hotkey[:8]} ({result.error or "reverted"})')
        return False
    bt.logging.info(f'relay: vote_unlock cast for {hotkey[:8]} at epoch {epoch}')
    dev_signal.emit('relay_unlock_voted', miner=miner, hotkey=hotkey, epoch=epoch)
    relay.mark_dirty(miner)
    return True


# ─── cadence true-up ─────────────────────────────────────────────────────────


def maybe_cadence_settle(relay, now: int) -> None:
    """Fire the global fee true-up once per aligned boundary."""
    cadence = relay.cfg.fee_cadence_secs
    boundary = (now // cadence) * cadence
    try:
        last = int(relay.store.get_relay_meta(_CADENCE_KEY) or 0)
    except ValueError:
        last = 0
    if boundary <= last:
        return

    entries = cadence_entries(relay, boundary)
    if not entries:
        relay.store.set_relay_meta(_CADENCE_KEY, boundary)
        return
    delta = _has_delta(relay, entries)
    if delta is None:
        return  # vault unreadable — retry at the next tick, still inside this boundary
    if not delta:
        bt.logging.debug(f'relay: fee cadence boundary {boundary} has zero delta — skipping the round')
        relay.store.set_relay_meta(_CADENCE_KEY, boundary)
        return

    if relay.read_only:
        bt.logging.info(f'relay: WOULD settle {len(entries)} fee entr(ies) at boundary {boundary} (read-only)')
        relay.store.set_relay_meta(_CADENCE_KEY, boundary)
        return
    for chunk in _chunks(entries, MAX_BATCH):
        result = relay.vault.vote_collect_fees_batch(chunk)
        if not result.ok:
            bt.logging.warning(f'relay: fee cadence batch rejected ({result.error or "reverted"})')
            return  # boundary stays unmarked so the next tick retries the whole round
    bt.logging.info(f'relay: fee true-up voted for {len(entries)} miner(s) at boundary {boundary}')
    dev_signal.emit('relay_fee_cadence', boundary=boundary, entries=len(entries))
    relay.store.set_relay_meta(_CADENCE_KEY, boundary)


def cadence_entries(relay, boundary: int) -> List[Tuple[str, int]]:
    """The batch vector: every miner with a fee accrued at-or-before the boundary, sorted by raw
    AccountId. Membership is derived from the Solana event stream ALONE — deliberately not
    filtered against the vault — because the vector is hashed into the round key and must be
    byte-identical across validators; the vault's monotonic totals no-op any stale entry."""
    generation = relay.vault_generation()
    if generation is None:
        return []
    totals: Dict[str, int] = relay.store.accrued_fee_totals(
        relay.backing, at_time=boundary, vault_generation=generation
    )
    entries: List[Tuple[bytes, str, int]] = []
    for miner, total in totals.items():
        if total <= 0:
            continue
        hotkey = relay.hotkey_for(miner)
        if hotkey is None:
            bt.logging.warning(f'relay: {miner[:8]} owes fees but has no binding — excluded from the true-up')
            continue
        entries.append((codec.account_bytes(hotkey), hotkey, total))
    entries.sort(key=lambda e: e[0])
    return [(hotkey, total) for _raw, hotkey, total in entries]


def _has_delta(relay, entries: Sequence[Tuple[str, int]]) -> Optional[bool]:
    """Whether any entry would actually move the vault's books. None when a read failed — the
    skip/fire decision has to be made on facts, not on a defaulted zero."""
    found = False
    for hotkey, total in entries:
        settled = relay.vault.get_settled_total(hotkey)
        if settled is None:
            return None
        if total > settled:
            found = True
    return found


def _chunks(entries: List[Tuple[str, int]], size: int):
    for i in range(0, len(entries), size):
        yield entries[i : i + size]

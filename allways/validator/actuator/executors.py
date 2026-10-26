"""How each actuator intent becomes a program transaction.

Sends return as soon as the transaction is submitted (preflight still runs, so a contract rejection
comes back at once); the actuator's ConfirmTracker settles them later in one batched status read. A lost
race or an already-cast vote is a benign no-op, returned as None."""

from __future__ import annotations

import base64
import time
from typing import Any, Callable, Dict, List, Optional

import bittensor as bt
from solders.hash import Hash
from solders.transaction import Transaction

from allways.constants import CANCEL_REASON_OTHER
from allways.solana import pdas
from allways.solana.client import AllwaysSolanaClient, benign_marker, swap_from_solana, swap_key_from_tx_hash
from allways.validator.actuator.swaps import _extend_target
from allways.validator.runtime.deadline_queue import Intent

# A peer already slid it, or it is at the ceiling.
BENIGN_EXTEND = ('ExtensionNotLater', 'ExtensionExceedsCeiling')
# A peer already reaped it, or the on-chain clock hasn't passed the expiry yet.
BENIGN_CLOSE = ('ClaimNotExpired', 'NotPending', 'AccountNotInitialized', 'could not find account')

# A peer resolved it first, the on-chain clock hasn't passed closes_at, or the draw's seed slot isn't out yet.
BENIGN_RESOLVE = ('NoRequests', 'PoolNotClosed', 'SeedSlotNotYetProduced')
# Already voted this round, the on-chain cadence floor, or de-whitelisted mid-attempt.
BENIGN_WEIGHTS = ('AlreadyVoted', 'WeightsUpdateTooSoon', 'NotValidator')

Executor = Callable[[Any, Intent], Optional[str]]


class StaleWorld(Exception):
    """The chain no longer matches the World the intent was planned on: send nothing, re-read ``keys``."""

    def __init__(self, keys) -> None:
        super().__init__(f'world stale for {sorted(keys)}')
        self.keys = set(keys)


class SendingClient(AllwaysSolanaClient):
    """The program client with a send that does not wait for confirmation."""

    def _send(
        self,
        instructions: List[Any],
        signers: Optional[List[Any]] = None,
        skip_preflight: bool = False,
        retries: int = 8,
    ) -> str:
        signers = signers or [self.keypair]
        tried: List[str] = []
        for attempt in range(retries):
            blockhash = Hash.from_string(self.rpc.get_latest_blockhash())
            tx = Transaction.new_signed_with_payer(instructions, self.keypair.pubkey(), signers, blockhash)
            try:
                return self.rpc.send_transaction(base64.b64encode(bytes(tx)).decode(), skip_preflight=skip_preflight)
            except Exception as e:
                # A lagging RPC can answer "blockhash not found" for a tx it still forwarded: before calling a
                # retry's rejection a failure, see whether an earlier attempt landed.
                landed = self._landed(tried)
                if landed is not None:
                    return landed
                msg = str(e).lower()
                if 'blockhash not found' not in msg and 'block height exceeded' not in msg:
                    raise
                tried.append(str(tx.signatures[0]))
                time.sleep(min(2.0, 0.25 * 2**attempt))
        raise RuntimeError(f'send failed after {retries} blockhash retries')

    def _landed(self, signatures: List[str]) -> Optional[str]:
        if not signatures:
            return None
        try:
            statuses = self.rpc.get_signature_statuses(signatures)
        except Exception:
            return None
        for sig, status in zip(signatures, statuses):
            if status is not None and status.get('err') is None:
                return sig
        return None


def _swap_key(swap: Any) -> bytes:
    return swap_key_from_tx_hash(swap.from_tx_hash)


def _voted(client: Any, req: int, swap: Any) -> bool:
    return client.has_voted(req, _swap_key(swap), client.keypair.pubkey())


def _as_planned(client: Any, swap: Any) -> None:
    """A vote that costs someone (a slash, a cancel, a reap) is cast only on the swap as it is on-chain now."""
    key = _swap_key(swap)
    raw = client.get_swap(key)
    if raw is None or swap_from_solana(raw, key) != swap:
        raise StaleWorld({('Swap', str(pdas.swap_pda(key, client.program_id)))})


def _benign(e: Exception, markers) -> bool:
    marker = benign_marker(e, markers)
    if marker:
        bt.logging.debug(f'benign no-op ({marker})')
    return bool(marker)


def vote_initiate(client: Any, intent: Intent) -> Optional[str]:
    swap = intent.payload.swap
    if _voted(client, pdas.REQ_INITIATE, swap):
        return None
    backing = str(getattr(swap, 'collateral_chain', 'sol') or 'sol').lower()
    try:
        return client.vote_initiate(
            _swap_key(swap),
            swap.miner,
            swap.from_chain,
            str(swap.user_from_addr),
            backing=backing,
            miner_from_addr=str(swap.miner_from_addr),
        )
    except Exception:
        _extend_after_refused_attest(client, swap)
        raise


def _extend_after_refused_attest(client: Any, swap: Any) -> None:
    """An attest the chain refused leaves a verified deposit at the miner with the clock running. Near expiry,
    slide the reservation so the re-planned attest gets another try instead of the claim being reaped (#813).
    Zero confirmations assumed (the longest bucket). Never raises: the refusal itself is what the caller reports."""
    try:
        reservation = client.get_reservation(swap.miner, str(swap.collateral_chain))
        if reservation is None:
            return
        target = _extend_target(
            swap.from_chain, 0, int(reservation.reserved_until), int(reservation.max_extend_at), int(time.time())
        )
        if not target:
            return
        client.extend_reservation(
            swap.miner,
            target,
            str(swap.collateral_chain),
            from_chain=swap.from_chain,
            from_addr=swap.user_from_addr,
            miner_from_addr=swap.miner_from_addr,
        )
        bt.logging.warning(
            f'vote_initiate {_swap_key(swap).hex()[:16]}: attest refused near expiry, extended reserved_until to {target}'
        )
    except Exception as e:
        if not benign_marker(e, BENIGN_EXTEND):
            bt.logging.error(
                f'vote_initiate {_swap_key(swap).hex()[:16]}: extension after a refused attest failed: {e}'
            )


def confirm_swap(client: Any, intent: Intent) -> Optional[str]:
    swap = intent.payload.swap
    if _voted(client, pdas.REQ_CONFIRM, swap):
        return None
    return client.confirm_swap(_swap_key(swap), swap.miner, swap.from_chain, swap.to_chain)


def timeout_swap(client: Any, intent: Intent) -> Optional[str]:
    swap = intent.payload.swap
    if _voted(client, pdas.REQ_TIMEOUT, swap):
        return None
    _as_planned(client, swap)
    return client.timeout_swap(_swap_key(swap), swap.miner, swap.user)


def cancel_swap(client: Any, intent: Intent) -> Optional[str]:
    swap = intent.payload.swap
    if _voted(client, pdas.REQ_CANCEL, swap):
        return None
    _as_planned(client, swap)
    return client.cancel_swap(_swap_key(swap), swap.miner, intent.payload.action.reason_code or CANCEL_REASON_OTHER)


def close_stale_claim(client: Any, intent: Intent) -> Optional[str]:
    swap = intent.payload.swap
    reservation_pda = pdas.reservation_pda(swap.miner, str(swap.collateral_chain), client.program_id)
    resv = client.get_reservation(swap.miner, str(swap.collateral_chain))
    if resv is not None and int(resv.reserved_until) >= time.time() and bytes(resv.claimed_swap_key) == _swap_key(swap):
        raise StaleWorld({('Reservation', str(reservation_pda))})  # still live on-chain: the World's copy is old
    try:
        return client.close_stale_claim(swap.miner, _swap_key(swap), str(swap.collateral_chain))
    except Exception as e:
        if _benign(e, BENIGN_CLOSE):
            return None
        raise


def extend_reservation(client: Any, intent: Intent) -> Optional[str]:
    swap, action = intent.payload.swap, intent.payload.action
    try:
        return client.extend_reservation(
            swap.miner,
            action.target_at,
            str(swap.collateral_chain),
            from_chain=swap.from_chain,
            from_addr=swap.user_from_addr,
            miner_from_addr=swap.miner_from_addr,
        )
    except Exception as e:
        if _benign(e, BENIGN_EXTEND):
            return None
        raise


def extend_timeout(client: Any, intent: Intent) -> Optional[str]:
    swap, action = intent.payload.swap, intent.payload.action
    try:
        return client.extend_timeout(_swap_key(swap), swap.miner, action.target_at)
    except Exception as e:
        if _benign(e, BENIGN_EXTEND):
            return None
        raise


def resolve_pool(client: Any, intent: Intent) -> Optional[str]:
    pool = intent.payload
    try:
        return client.resolve_pool(pool.miner, str(getattr(pool, 'collateral_chain', 'sol') or 'sol'))
    except Exception as e:
        if _benign(e, BENIGN_RESOLVE):
            return None
        raise


# The config field holding each hub's collateral floor.
_FLOOR_FIELDS = {'sol': 'min_collateral', 'tao': 'tao_min_collateral'}


def _under_floor(client: Any, miner: str, backing: str) -> None:
    """A floor kick is cast only while the hub, read live, is still lit and its purse under the floor: the
    local vault for SOL, a locked attested bond for a vault hub."""
    ms, config = client.get_miner_state(miner), client.get_config()
    floor = int(getattr(config, _FLOOR_FIELDS.get(backing, ''), 0) or 0)
    purse = None
    if ms is not None and ms.active and int(ms.active_backings or 0) & pdas.BACKING_BITS[backing]:
        if backing == 'sol':
            purse = int(ms.collateral)
        else:
            att = client.get_bond_attestation(miner, backing)
            purse = int(att.effective_balance) if att is not None and att.locked else None
    if purse is None or not floor or purse >= floor:
        keys = {('MinerState', str(pdas.miner_state_pda(miner, client.program_id)))}
        if backing != 'sol':
            keys.add(('BondAttestation', str(pdas.bond_attestation_pda(miner, backing, client.program_id))))
        raise StaleWorld(keys)


def vote_deactivate(client: Any, intent: Intent) -> Optional[str]:
    miner, backing = intent.payload
    if client.has_voted(pdas.REQ_DEACTIVATE, miner, client.keypair.pubkey()):
        return None
    _under_floor(client, miner, backing)
    try:
        return client.vote_deactivate(miner, backing=backing)
    except Exception as e:
        if _benign(e, ('AlreadyVoted',)):
            return None
        raise


def vote_set_weights(client: Any, intent: Intent) -> Optional[str]:
    vector, keys = intent.payload
    try:
        return client.vote_set_weights(vector, keys)
    except Exception as e:
        if _benign(e, BENIGN_WEIGHTS):
            return None
        raise


def vote_set_attestation(client: Any, intent: Intent) -> Optional[str]:
    """A held hub's refusal (AttestationWouldStrandSwap) raises, so the relay's vote is retried once the
    hub frees; an already-recorded vote is a no-op."""
    miner, chain, balance, locked, epoch = intent.payload
    try:
        return client.vote_set_attestation(miner, chain, balance, locked, epoch)
    except Exception as e:
        if _benign(e, ('AlreadyVoted',)):
            return None
        raise


def vote_attest_heartbeat(client: Any, intent: Intent) -> Optional[str]:
    if client.has_voted(pdas.REQ_ATTEST_HEARTBEAT, pdas.config_pda(client.program_id), client.keypair.pubkey()):
        return None
    try:
        return client.vote_attest_heartbeat()
    except Exception as e:
        if _benign(e, ('AlreadyVoted',)):
            return None
        raise


SWAP_EXECUTORS: Dict[str, Executor] = {
    'vote_initiate': vote_initiate,
    'confirm_swap': confirm_swap,
    'timeout_swap': timeout_swap,
    'cancel_swap': cancel_swap,
    'close_stale_claim': close_stale_claim,
    'extend_reservation': extend_reservation,
    'extend_timeout': extend_timeout,
}


EXECUTORS: Dict[str, Executor] = {
    **SWAP_EXECUTORS,
    'resolve_pool': resolve_pool,
    'vote_deactivate': vote_deactivate,
    'vote_set_weights': vote_set_weights,
    'vote_set_attestation': vote_set_attestation,
    'vote_attest_heartbeat': vote_attest_heartbeat,
}

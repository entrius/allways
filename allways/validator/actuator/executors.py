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
from allways.solana.client import AllwaysSolanaClient, benign_marker, swap_key_from_tx_hash
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
        for attempt in range(retries):
            blockhash = Hash.from_string(self.rpc.get_latest_blockhash())
            tx = Transaction.new_signed_with_payer(instructions, self.keypair.pubkey(), signers, blockhash)
            try:
                return self.rpc.send_transaction(base64.b64encode(bytes(tx)).decode(), skip_preflight=skip_preflight)
            except Exception as e:  # only a stale blockhash is retried: that tx never entered the ledger
                msg = str(e).lower()
                if 'blockhash not found' not in msg and 'block height exceeded' not in msg:
                    raise
                time.sleep(min(2.0, 0.25 * 2**attempt))
        raise RuntimeError(f'send failed after {retries} blockhash retries')


def _swap_key(swap: Any) -> bytes:
    return swap_key_from_tx_hash(swap.from_tx_hash)


def _voted(client: Any, req: int, swap: Any) -> bool:
    return client.has_voted(req, _swap_key(swap), client.keypair.pubkey())


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
    return client.vote_initiate(
        _swap_key(swap),
        swap.miner,
        swap.from_chain,
        str(swap.user_from_addr),
        backing=backing,
        miner_from_addr=str(swap.miner_from_addr),
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
    return client.timeout_swap(_swap_key(swap), swap.miner, swap.user)


def cancel_swap(client: Any, intent: Intent) -> Optional[str]:
    swap = intent.payload.swap
    if _voted(client, pdas.REQ_CANCEL, swap):
        return None
    return client.cancel_swap(_swap_key(swap), swap.miner, intent.payload.action.reason_code or CANCEL_REASON_OTHER)


def close_stale_claim(client: Any, intent: Intent) -> Optional[str]:
    swap = intent.payload.swap
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


def vote_deactivate(client: Any, intent: Intent) -> Optional[str]:
    miner, backing = intent.payload
    if client.has_voted(pdas.REQ_DEACTIVATE, miner, client.keypair.pubkey()):
        return None
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

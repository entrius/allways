"""One coldkey signer: one lock, one nonce keeper, a short era, a head-age gate, so no retry can double-pay."""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from allways.miner.alpha.logbook import get_logger

INCLUDED, DEAD, WAIT = 'included', 'dead', 'wait'
POOL_REJECTED = 'PoolRejected'  # the node refused the bytes at submit: nothing entered the pool
_POOL_REJECTIONS = ('Invalid Transaction', 'Priority is too low')  # substrate author RPC errors 1010 and 1014
HEAD_STALE_ALERT = 'subtensor-head-stale'
_MAX_RETRIES = 1  # the SDK resubmits the same bytes on a socket drop: one attempt, then the nonce decides
_RETRY_TIMEOUT_SECS = 20
_LOGGER = get_logger('subtensor_signer')


@dataclass(frozen=True)
class Signed:
    nonce: int
    tx_hash: str
    head: int


@dataclass(frozen=True)
class Included:
    tx_hash: str
    block: int
    block_hash: str
    index: int
    success: bool
    error: str
    fee_rao: int


class SubtensorSigner:
    """Every coldkey send: lock → nonce above the last recorded tx that can still land → record → one submit."""

    def __init__(
        self, connect: Callable[[str | None], Any], wallet, period: int = 16, head_max_age_secs: int = 60, alert=None
    ):
        self.lock = threading.RLock()
        self.connect = connect  # receives the endpoint just dropped, so the next socket can rotate off it
        self.wallet = wallet
        self.period = period
        self.head_max_age_secs = head_max_age_secs
        self.alert = alert
        self._subtensor = None
        self._dropped_endpoint = None
        self._last_recorded: Signed | None = None
        self._last_included = -1

    @property
    def coldkey(self) -> str:
        return self.wallet.coldkeypub.ss58_address

    def compose(self, function: str, params: dict) -> Any:
        return self._on_socket(lambda s: s.compose_call('SubtensorModule', function, params))

    def head_fresh(self) -> bool:
        """Timestamp.Now within head_max_age_secs of the wall clock. Unreadable or stale drops the socket."""
        try:
            now_ms = self._on_socket(lambda s: s.query('Timestamp', 'Now').value)
        except Exception as error:
            _LOGGER.warning('Head timestamp unreadable error=%s', error)
            return False
        if time.time() - int(now_ms) / 1000 <= self.head_max_age_secs:
            return True
        self._drop()  # a frozen endpoint: the next call reconnects and may rotate off it
        return False

    def send(self, call, record: Callable[[Signed], None]) -> tuple[Signed, Included | None] | None:
        """Under the lock: head gate → sign → record → submit once. None = nothing was sent."""
        with self.lock:
            if not self.head_fresh():
                self._alert_stale()
                return None
            try:
                signed, extrinsic = self.sign(call)
            except Exception as error:
                _LOGGER.warning('Sign failed error=%s', error)
                return None
            record(signed)
            recorded, self._last_recorded = self._last_recorded, signed
            included = self.submit(extrinsic)
            if included is None:
                return signed, None
            if included.error == POOL_REJECTED:
                self._last_recorded = recorded  # never in the pool: the nonce floor stays where it was
            else:
                self._last_included = max(self._last_included, signed.nonce)
            return signed, included

    def sign(self, call) -> tuple[Signed, Any]:
        """nonce = max(next index, floor); mortal for `period` blocks from the finalised head, recorded as head."""
        floor = self._nonce_floor()

        def on(substrate):
            nonce = max(substrate.get_account_next_index(self.coldkey), floor)
            birth = substrate.get_block_number(substrate.get_chain_finalised_head())
            era = {'period': self.period, 'current': birth}
            extrinsic = substrate.create_signed_extrinsic(call, self.wallet.coldkey, era=era, nonce=nonce)
            return Signed(nonce, '0x' + extrinsic.extrinsic_hash.hex(), birth), extrinsic

        return self._on_socket(on)

    def _nonce_floor(self) -> int:
        """Last recorded + 1 while its era can still land (unreadable counts as alive); else last included + 1."""
        pending = self._last_recorded
        if pending is not None and pending.nonce > self._last_included and self.expired(pending) is not True:
            return pending.nonce + 1
        return self._last_included + 1

    def submit(self, extrinsic) -> Included | None:
        """wait_for_inclusion only. A pool rejection is a clean refusal; None = a lost answer: resolve, never resend."""
        tx_hash = '0x' + extrinsic.extrinsic_hash.hex()
        try:
            return self._on_socket(lambda s: _included(s, s.submit_extrinsic(extrinsic, wait_for_inclusion=True)))
        except Exception as error:
            if any(text in str(error) for text in _POOL_REJECTIONS):
                _LOGGER.warning('Submit rejected by the pool tx_id=%s error=%s', tx_hash, error)
                return Included(tx_hash, 0, '', -1, False, POOL_REJECTED, 0)
            _LOGGER.warning('Submit ambiguous tx_id=%s error=%s', tx_hash, error)
            return None

    def nonce_used(self, nonce: int) -> bool | None:
        """The coldkey's on-chain nonce passed ours; None = unreadable."""
        try:
            return self._on_socket(lambda s: s.query('System', 'Account', [self.coldkey]).value['nonce']) > nonce
        except Exception as error:
            _LOGGER.warning('Account nonce unreadable error=%s', error)
            return None

    def expired(self, signed: Signed) -> bool | None:
        """The finalised head is past the era, so no fork can still carry the tx; None = unreadable."""
        try:
            return self._finalised() > signed.head + self.period
        except Exception as error:
            _LOGGER.warning('Finalised head unreadable error=%s', error)
            return None

    def find(self, tx_hash: str, from_block: int) -> Included | None:
        """Scan from_block .. from_block + period + 1 once finalised, so no reorg hides a landed tx. None is proof."""
        last = from_block + self.period + 1
        if self._finalised() < last:
            raise LookupError(f'block {last} is not finalised')
        for number in range(from_block, last + 1):
            block = self._on_socket(lambda s: s.get_block(block_number=number))
            if block is None:
                raise LookupError(f'block {number} is not readable')
            for index, extrinsic in enumerate(block['extrinsics']):
                if extrinsic is not None and '0x' + extrinsic.extrinsic_hash.hex() == tx_hash:
                    block_hash = block['header']['hash']
                    return self._on_socket(lambda s: _included(s, s.retrieve_extrinsic_by_hash(block_hash, tx_hash)))
        return None

    def resolve(self, signed: Signed) -> tuple[str, Included | None]:
        """A submitted extrinsic's fate. Any unknown read waits; only a fully scanned era says dead."""
        used = self.nonce_used(signed.nonce)
        if used is None:
            return WAIT, None
        if used:
            try:
                found = self.find(signed.tx_hash, signed.head)
            except Exception as error:
                _LOGGER.warning('Era scan incomplete tx_id=%s error=%s', signed.tx_hash, error)
                return WAIT, None
            return (INCLUDED, found) if found else (DEAD, None)
        return (DEAD, None) if self.expired(signed) is True else (WAIT, None)

    def _substrate(self):
        if self._subtensor is None:
            self._subtensor = self.connect(self._dropped_endpoint)
            self._subtensor.substrate.max_retries = _MAX_RETRIES
            self._subtensor.substrate.retry_timeout = _RETRY_TIMEOUT_SECS
        return self._subtensor.substrate

    def _on_socket(self, operation: Callable[[Any], Any]) -> Any:
        """One socket operation under the lock (the sync websocket is single-reader); a raise drops the connection."""
        with self.lock:
            try:
                return operation(self._substrate())
            except Exception:
                self._drop()
                raise

    def _finalised(self) -> int:
        return self._on_socket(lambda s: s.get_block_number(s.get_chain_finalised_head()))

    def _drop(self) -> None:
        subtensor, self._subtensor = self._subtensor, None
        if subtensor is None:
            return
        self._dropped_endpoint = getattr(subtensor, 'chain_endpoint', None)
        try:
            subtensor.close()
        except Exception as error:
            _LOGGER.debug('Socket close failed error=%s', error)

    def _alert_stale(self) -> None:
        _LOGGER.warning('Subtensor head older than %s s — no coldkey send', self.head_max_age_secs)
        if self.alert is None:
            return
        try:
            self.alert(HEAD_STALE_ALERT, 'Subtensor head is stale', 'Coldkey sends wait until the head is fresh.')
        except Exception as error:
            _LOGGER.warning('Stale-head alert failed: %s', error)


def _included(substrate, receipt) -> Included:
    """A receipt read to the end; an unreadable outcome raises, so a lost read never reports a failure."""
    success = receipt.is_success
    if success is None:
        raise LookupError(f'events unreadable for {receipt.extrinsic_hash}')
    block = receipt.block_number
    if block is None:
        block = substrate.get_block_number(receipt.block_hash)
    error = receipt.error_message  # {'type', 'name', 'docs'} on a failed dispatch
    return Included(
        tx_hash=receipt.extrinsic_hash,
        block=int(block),
        block_hash=receipt.block_hash,
        index=int(receipt.extrinsic_idx),
        success=bool(success),
        error=str(error['name']) if error else '',
        fee_rao=int(receipt.total_fee_amount or 0),
    )


def guard_coldkey(assets: dict, signer: SubtensorSigner) -> None:
    """Every TAO send shares the coldkey with the signer: hold its lock around assets['tao'].send_amount."""
    tao = assets.get('tao')
    if tao is None:
        return
    send_amount = tao.send_amount

    def locked_send(to_address, amount, from_address=None, dedup_key=None):
        with signer.lock:
            return send_amount(to_address, amount, from_address=from_address, dedup_key=dedup_key)

    tao.send_amount = locked_send

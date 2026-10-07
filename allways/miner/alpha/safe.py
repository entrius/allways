"""Alpha delivery: every send journaled before submit and signed through the one coldkey signer."""

from types import SimpleNamespace

from bittensor.core.types import ExtrinsicResponse

from allways.assets.alpha import ALPHA_WHOLE_POSITION, Alpha
from allways.miner.alpha.journal import HELD, SETTLED, XFER_SENT, AlphaJournal
from allways.miner.alpha.logbook import get_logger
from allways.miner.alpha.signer import INCLUDED, WAIT, Signed, SubtensorSigner

_LOGGER = get_logger('safe_alpha')


class RowLeftHeld(RuntimeError):
    """The router re-sized the row between the free-alpha check and the record: nothing may reach the chain."""


class SafeAlpha(Alpha):
    """Journal before submit, never resend while a recorded transfer can land, the exact amount from one hotkey."""

    def __init__(self, chain_def, subtensor, wallet, signer: SubtensorSigner, journal: AlphaJournal):
        super().__init__(chain_def, subtensor, wallet)
        self.signer = signer
        self.journal = journal
        self._swap_key = None  # the swap being delivered; one delivery thread

    def send_amount(self, to_address, amount, from_address=None, dedup_key=None):
        """A settled row is the receipt, a recorded transfer resolves first, only a HELD row lets allways' send run."""
        self._swap_key = dedup_key
        row = self.journal.row(dedup_key) if dedup_key else None
        if row is not None and row['state'] == XFER_SENT:
            row = self.settle_recorded(dedup_key, row['xfer'])
        if row is not None and row['state'] == SETTLED:
            return row['xfer']['tx_hash'], row['xfer']['block']
        if row is None or row['state'] != HELD:
            _LOGGER.warning('No alpha send key=%s state=%s', dedup_key, row and row['state'])
            return None
        return super().send_amount(to_address, amount, from_address=from_address, dedup_key=dedup_key)

    def settle_recorded(self, key, xfer) -> dict:
        """The signer's verdict on the row: landed → SETTLED, failed or dead → HELD, unknown → unchanged."""
        signed = Signed(nonce=xfer['nonce'], tx_hash=xfer['tx_hash'], head=xfer['head'])
        verdict, included = self.signer.resolve(signed)
        if verdict == WAIT:
            return self.journal.row(key)
        if verdict == INCLUDED and included.success:
            _LOGGER.info('Recorded transfer landed key=%s tx_id=%s block=%s', key, signed.tx_hash, included.block)
            return self.journal.update(
                key, state=SETTLED, xfer=xfer | {'block': included.block, 'index': included.index}
            )
        _LOGGER.warning('Recorded transfer moved nothing key=%s tx_id=%s verdict=%s', key, signed.tx_hash, verdict)
        return self.journal.update(key, state=HELD)

    def transfer(self, to_address, origin_hotkey, destination_hotkey, amount):
        """Compose, then signer.send with the journal write as its record step; the response mirrors the SDK's."""
        amount, key = int(amount), self._swap_key
        if amount >= ALPHA_WHOLE_POSITION:
            return self._refused('whole-position sentinel is not an amount')
        row = self.journal.row(key) if key else None
        if row is None or row['state'] != HELD or row['need'] != amount:
            return self._refused(f'no HELD alpha journal row needing {amount} for {key}')
        try:
            free = self.free_to_send(self.signer.coldkey)
            if free < amount:
                return self._refused(f'only {free} of {amount} alpha free to send')
            call = self.signer.compose(
                *_transfer_call(self.netuid, to_address, origin_hotkey, destination_hotkey, amount)
            )
        except Exception as error:
            return self._refused(f'not sent: {error}')

        def record(signed: Signed) -> None:
            if not self.journal.transition(key, HELD, state=XFER_SENT, xfer=_xfer(signed)):
                raise RowLeftHeld(key)

        try:
            sent = self.signer.send(call, record)
        except RowLeftHeld:
            return self._refused('not sent: the row left HELD before the record')
        if sent is None:
            return self._refused('not sent: stale head or sign failed')
        signed, included = sent
        if included is None:
            return _response(False, 'submit ambiguous — resolved on the next poll', signed.tx_hash)
        if not included.success:
            self.journal.update(key, state=HELD)
            return _response(False, included.error, signed.tx_hash, included.block_hash)
        self.journal.update(key, state=SETTLED, xfer=_xfer(signed) | {'block': included.block, 'index': included.index})
        return _response(True, 'included', signed.tx_hash, included.block_hash)

    def _refused(self, message: str):
        """Nothing reached the chain: drop allways' attempt record so the next poll may send."""
        self.broadcasted_txids.pop(self._swap_key or '', None)
        return _response(False, message)


def _xfer(signed: Signed) -> dict:
    return {'nonce': signed.nonce, 'tx_hash': signed.tx_hash, 'head': signed.head}


def _transfer_call(netuid: int, to_address, origin_hotkey, destination_hotkey, amount: int) -> tuple[str, dict]:
    """transfer_stake keeps the hotkey; transfer_stake_and_hotkey lands on one the recipient can take."""
    netuids = {'origin_netuid': netuid, 'destination_netuid': netuid, 'alpha_amount': amount}
    if origin_hotkey == destination_hotkey:
        return 'transfer_stake', {'destination_coldkey': to_address, 'hotkey': origin_hotkey, **netuids}
    hotkeys = {'origin_hotkey': origin_hotkey, 'destination_hotkey': destination_hotkey}
    return 'transfer_stake_and_hotkey', {'destination_coldkey': to_address, **hotkeys, **netuids}


def _response(success: bool, message: str, tx_hash: str | None = None, block_hash: str | None = None):
    """An ExtrinsicResponse as allways' send_amount reads it: success, message and the receipt's hashes."""
    receipt = SimpleNamespace(extrinsic_hash=tx_hash, block_hash=block_hash) if tx_hash else None
    return ExtrinsicResponse(success=success, message=message, extrinsic_receipt=receipt)

from hashlib import blake2b
from typing import Any, Dict, Optional, Tuple

import bittensor as bt
from bittensor import Keypair
from bittensor.utils import is_valid_ss58_address, ss58_encode

from allways.assets.base import Asset, ProviderUnreachableError, TransactionInfo
from allways.assets.chain import Chain
from allways.chains import CHAIN_TAO, ChainDefinition

LOG_SUB = '[Subtensor]'


class Tao(Asset, Chain):
    """TAO chain provider using bt.Subtensor and substrate-interface.

    Owns its signing ``bt.Wallet`` when one is supplied at construction, so
    callers of ``send_amount`` never need to pass key material. Validators
    and read-only consumers can instantiate without a wallet and will get a
    clear error if they attempt to send.
    """

    # RPCs run on the shared substrate websocket — callers serialise via axon_lock.
    uses_substrate = True

    # Balances pallet index and transfer call indices on Subtensor
    _BALANCES_PALLET = 5
    _TRANSFER_CALLS = {0: 'transfer_allow_death', 3: 'transfer_keep_alive', 7: 'transfer_all'}

    def __init__(self, subtensor: bt.Subtensor, wallet: Optional['bt.Wallet'] = None):
        self.subtensor = subtensor
        self.wallet = wallet
        self.block_cache: Dict[int, dict] = {}
        self.block_hash_cache: Dict[int, str] = {}
        self.events_cache: Dict[str, list] = {}
        # Deposit-scanner head cursors, keyed per (from, to, amount) triple — see find_recent_outgoing.
        self.scan_cursors: Dict[Tuple[str, str, int], int] = {}

    def get_chain(self) -> ChainDefinition:
        return CHAIN_TAO

    def describe(self) -> str:
        return f'Subtensor {self.subtensor.chain_endpoint}'

    def can_send_from(self, address: str) -> bool:
        return self.wallet is not None and self.wallet.coldkeypub.ss58_address == address

    def check_connection(self, **kwargs) -> None:
        try:
            block = self.subtensor.get_current_block()
            bt.logging.success(f'{LOG_SUB} connected: block={block}')
        except Exception as e:
            raise ConnectionError(f'Cannot reach Subtensor: {e}') from e

    def clear_cache(self):
        """Clear the block cache. Call at the start of each poll cycle."""
        self.block_cache.clear()
        self.block_hash_cache.clear()
        self.events_cache.clear()

    @staticmethod
    def decode_compact(data: bytes) -> Tuple[int, int]:
        """Decode a SCALE compact integer. Returns (value, bytes_consumed)."""
        if not data:
            return 0, 0
        mode = data[0] & 0x03
        if mode == 0:
            return data[0] >> 2, 1
        elif mode == 1:
            if len(data) < 2:
                return 0, 0
            return (data[0] | (data[1] << 8)) >> 2, 2
        elif mode == 2:
            if len(data) < 4:
                return 0, 0
            return (int.from_bytes(data[:4], 'little')) >> 2, 4
        else:
            n = (data[0] >> 2) + 4
            if len(data) < 1 + n:
                return 0, 0
            return int.from_bytes(data[1 : 1 + n], 'little'), 1 + n

    @classmethod
    def parse_raw_extrinsic(cls, ext_hex: str) -> Optional[dict]:
        """Parse a raw SCALE-encoded extrinsic hex string to extract transfer info."""
        try:
            raw = bytes.fromhex(ext_hex[2:] if ext_hex.startswith('0x') else ext_hex)
            ext_hash = '0x' + blake2b(raw, digest_size=32).hexdigest()

            # Decode compact length prefix
            _, length_bytes = cls.decode_compact(raw)
            body = raw[length_bytes:]
            if not body:
                return None

            # Check if signed (first byte & 0x80)
            if not (body[0] & 0x80):
                return None

            # body[1] is the MultiAddress variant; only Id (0x00) carries a bare AccountId, at
            # body[2:34]. Reading from body[1] straddles the variant byte and yields a shifted
            # (valid-looking but wrong) ss58, so anything else fails closed.
            if len(body) < 34 or body[1] != 0x00:
                return None
            sender = ss58_encode(body[2:34], ss58_format=42)

            # Find the transfer call: pallet_index=5, call_index in {0,3,7}
            # The call data follows the signature block. Instead of parsing the full
            # signature, search for the Balances pallet marker after the signature.
            # Signature occupies ~65 bytes (1 type + 64 sig) + era + nonce + tip
            # We search from offset 33 onward for pallet 5 + valid call index.
            call_offset = None
            for i in range(33, len(body) - 35):
                if body[i] == cls._BALANCES_PALLET and body[i + 1] in cls._TRANSFER_CALLS:
                    # Verify: next byte should be MultiAddress variant 0x00 (Id)
                    if i + 2 < len(body) and body[i + 2] == 0x00:
                        call_offset = i
                        break

            if call_offset is None:
                return None

            call_idx = body[call_offset + 1]
            after_call = body[call_offset + 2 :]

            # MultiAddress::Id = 0x00 + 32 bytes AccountId
            if len(after_call) < 33 or after_call[0] != 0x00:
                return None
            dest_bytes = after_call[1:33]
            dest = ss58_encode(dest_bytes, ss58_format=42)

            # Compact<Balance> follows
            amount, _ = cls.decode_compact(after_call[33:])

            return {
                'extrinsic_hash': ext_hash,
                'call_function': cls._TRANSFER_CALLS[call_idx],
                'sender': sender,
                'dest': dest,
                'amount': amount,
            }
        except Exception:
            return None

    def get_block_hash(self, block_num: int) -> Optional[str]:
        """Block hash for a height, cached per poll cycle (settlement checks re-need it)."""
        if block_num in self.block_hash_cache:
            return self.block_hash_cache[block_num]
        block_hash = self.subtensor.substrate.get_block_hash(block_num)
        if block_hash:
            self.block_hash_cache[block_num] = block_hash
        return block_hash

    def get_block(self, block_num: int) -> Optional[dict]:
        """Fetch a block, using cache to avoid redundant RPC calls within a poll cycle."""
        if block_num in self.block_cache:
            return self.block_cache[block_num]

        block_hash = self.get_block_hash(block_num)
        if not block_hash:
            return None

        try:
            block = self.subtensor.substrate.get_block(block_hash)
            if block:
                self.block_cache[block_num] = block
            return block
        except Exception as e:
            bt.logging.debug(f'Block fetch failed for block {block_num}, falling back to raw: {e}')

        # Fallback: raw RPC for blocks with pruned state
        return self.get_block_raw(block_num, block_hash)

    def get_block_raw(self, block_num: int, block_hash: str) -> Optional[dict]:
        """Fetch a block via raw RPC and parse transfer extrinsics manually."""
        try:
            result = self.subtensor.substrate.rpc_request('chain_getBlock', [block_hash])
            raw_block = result.get('result', {}).get('block', {})
            raw_exts = raw_block.get('extrinsics', [])

            # Keep the true position: this list is filtered to transfers, but the settlement
            # check keys events on the extrinsic's index within the full block.
            parsed_exts = []
            for idx, ext_hex in enumerate(raw_exts):
                parsed = self.parse_raw_extrinsic(ext_hex)
                if parsed:
                    parsed['extrinsic_idx'] = idx
                    parsed_exts.append(parsed)

            block = {'extrinsics': parsed_exts, '_raw': True}
            self.block_cache[block_num] = block
            return block
        except Exception as e:
            bt.logging.debug(f'Raw block fetch failed for block {block_num}: {e}')
            return None

    def get_block_events(self, block_hash: str) -> list:
        """System.Events for a block, cached per poll cycle.

        Raises ProviderUnreachableError when events can't be read: callers must treat that as
        'unknown', never as 'no transfer'. Returning None here would map to a slash-eligible
        verdict on a node hiccup.
        """
        if block_hash in self.events_cache:
            return self.events_cache[block_hash]
        try:
            events = self.subtensor.substrate.get_events(block_hash)
        except Exception as e:
            raise ProviderUnreachableError(f'TAO events unavailable for {block_hash[:16]}...: {e}') from e
        if events is None:
            raise ProviderUnreachableError(f'TAO events unavailable for {block_hash[:16]}...')
        events = list(events)
        self.events_cache[block_hash] = events
        return events

    @staticmethod
    def _event_extrinsic_idx(record: Any) -> Optional[int]:
        """Index of the extrinsic that emitted this event record, or None if not extrinsic-applied."""
        if not isinstance(record, dict):
            return None
        idx = record.get('extrinsic_idx')
        if isinstance(idx, int):
            return idx
        phase = record.get('phase')
        if isinstance(phase, dict):
            applied = phase.get('ApplyExtrinsic')
            if isinstance(applied, int):
                return applied
        return None

    @classmethod
    def _transfer_from_event(cls, record: Any) -> Optional[Tuple[str, str, int]]:
        """(sender, dest, amount) if this record is a Balances.Transfer, else None.

        Tolerates the shapes scalecodec emits across runtime/metadata versions: flat
        module_id/event_id with dict or positional attributes, and the nested
        {'Balances': {'Transfer': ...}} variant form.
        """
        if not isinstance(record, dict):
            return None
        event = record.get('event', record)
        if not isinstance(event, dict):
            return None

        attributes: Any = None
        module = event.get('module_id') or event.get('module') or event.get('pallet')
        name = event.get('event_id') or event.get('event') or event.get('name')
        if isinstance(module, str) and isinstance(name, str):
            if module != 'Balances' or name != 'Transfer':
                return None
            attributes = event.get('attributes', event.get('params'))
        else:
            balances = event.get('Balances')
            if not isinstance(balances, dict) or 'Transfer' not in balances:
                return None
            attributes = balances['Transfer']

        # Past this point the record IS a Balances.Transfer, so a payload we can't read is
        # 'unknown', never 'absent'. Returning None here would read a real deposit as moving
        # no funds — on a dest leg that is slash-eligible, so shape drift would false-slash.
        def unreadable(detail: str) -> ProviderUnreachableError:
            return ProviderUnreachableError(f'unreadable Balances.Transfer payload ({detail}): {attributes!r:.200}')

        if isinstance(attributes, dict):
            sender = attributes.get('from')
            dest = attributes.get('to')
            amount = attributes.get('amount', attributes.get('value'))
        elif isinstance(attributes, (list, tuple)) and len(attributes) >= 3:
            sender, dest, amount = attributes[0], attributes[1], attributes[2]
            # Older shapes wrap each param as {'name': ..., 'value': ...}.
            if isinstance(sender, dict):
                sender, dest, amount = (p.get('value') for p in (sender, dest, amount))
        else:
            raise unreadable(f'{type(attributes).__name__}')

        sender = cls._as_ss58(sender)
        dest = cls._as_ss58(dest)
        if not sender or not dest:
            raise unreadable('unresolved from/to')
        try:
            return sender, dest, int(amount)
        except (TypeError, ValueError) as e:
            raise unreadable('non-numeric amount') from e

    @staticmethod
    def _as_ss58(value: Any) -> str:
        """Normalise an AccountId event field (ss58 str, {'Id': ...}, or raw bytes) to ss58."""
        if isinstance(value, dict):
            value = value.get('Id', value.get('value'))
        if isinstance(value, (bytes, bytearray)) and len(value) == 32:
            return ss58_encode(bytes(value), ss58_format=42)
        if isinstance(value, str):
            if value.startswith('0x') and len(value) == 66:
                return ss58_encode(bytes.fromhex(value[2:]), ss58_format=42)
            return value
        return ''

    def settled_credit(self, block_num: int, extrinsic_idx: int, recipient: str) -> Optional[Tuple[str, int]]:
        """(sender, credited_rao) actually paid to ``recipient`` by this extrinsic, else None.

        Inclusion in a block is not settlement: a signed transfer that dispatches with an error
        (e.g. insufficient balance) still occupies a block and still decodes to the intended
        dest/amount. Only a Balances.Transfer event proves funds moved, so the event — not the
        call — is what the amount and sender are read from.
        """
        block_hash = self.get_block_hash(block_num)
        if not block_hash:
            raise ProviderUnreachableError(f'TAO block hash unavailable for {block_num}')

        # Reached only once an extrinsic was located in this block, so the block provably holds one
        # and must emit its ApplyExtrinsic records. An empty list is a broken response, not an
        # empty block, and reporting it as no-credit would false-slash on an RPC hiccup alone.
        events = self.get_block_events(block_hash)
        if not events:
            raise ProviderUnreachableError(f'no events returned for block {block_num}, which holds extrinsics')

        credited = 0
        sender = ''
        indexed = 0
        for record in events:
            idx = self._event_extrinsic_idx(record)
            if idx is None:
                continue
            indexed += 1
            if idx != extrinsic_idx:
                continue
            transfer = self._transfer_from_event(record)
            if transfer is None:
                continue
            ev_sender, ev_dest, ev_amount = transfer
            if ev_dest != recipient:
                continue
            credited += ev_amount
            sender = sender or ev_sender

        # Recognising none of them means the phase shape moved, not that nothing was applied.
        if not indexed:
            raise ProviderUnreachableError(
                f'no ApplyExtrinsic phase recognised in {len(events)} events at block {block_num}'
            )

        if credited <= 0:
            return None
        return sender, credited

    def get_block_time(self, block_num: int) -> Optional[int]:
        """Block's mined time in unix seconds, via the Timestamp pallet at that block hash (millis ÷ 1000).

        Used by the B2 replay-freshness checks (the substrate block carries no time on the parsed dict).
        """
        try:
            block_hash = self.subtensor.substrate.get_block_hash(block_num)
            if not block_hash:
                return None
            result = self.subtensor.substrate.query('Timestamp', 'Now', block_hash=block_hash)
            millis = int(getattr(result, 'value', result))
            return millis // 1000
        except Exception as e:
            bt.logging.debug(f'{LOG_SUB} block_time fetch failed for block {block_num}: {e}')
            return None

    def fetch_matching_tx(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        max_scan_blocks: int = 150,
    ) -> Optional[TransactionInfo]:
        """Scan for a TAO transfer matching recipient + amount.

        If block_hint > 0, checks the hinted block ±3. Otherwise scans
        ``max_scan_blocks`` back from current (newest first). The ±3 window
        covers small clock/finality skews between the caller's hint and the
        block the transfer actually landed in.

        Raises ProviderUnreachableError if subtensor is unreachable.
        """
        try:
            current_block = self.subtensor.get_current_block()
        except Exception as e:
            raise ProviderUnreachableError(f'Subtensor unreachable: {e}') from e

        if block_hint > 0:
            blocks_to_check = [block_hint + offset for offset in range(-3, 4) if block_hint + offset >= 0]
        else:
            window = max(1, int(max_scan_blocks))
            blocks_to_check = [current_block - offset for offset in range(window) if current_block - offset >= 0]

        # --- F1 diagnostics (2026-07-12): a delivered TAO leg was reported NOT FOUND -> miner slashed.
        # A hinted scan has no wide-scan fallback, so it silently fails whenever the node cannot see the
        # hinted blocks. Log enough to tell the three cases apart when it recurs:
        #   (a) head < hint      -> our node hasn't imported the block yet (stale view, NOT the miner's fault)
        #   (b) blocks all None  -> node couldn't serve the hinted blocks (stale view / RPC gap)
        #   (c) blocks retrieved, tx absent -> the HINT itself points at the wrong blocks
        bt.logging.debug(
            f'{LOG_SUB} verify tx={tx_hash[:16]}… hint={block_hint} head={current_block} '
            f'window=[{blocks_to_check[0]}..{blocks_to_check[-1]}] want>={expected_amount} to={expected_recipient[:8]}…'
        )
        if block_hint > 0 and current_block < block_hint:
            bt.logging.warning(
                f'{LOG_SUB} verify: node HEAD {current_block} is BEHIND hinted block {block_hint} by '
                f'{block_hint - current_block} — dest leg not visible yet; a NOT-FOUND here is a stale-view '
                f'false negative, not an absent tx. tx={tx_hash[:16]}…'
            )

        try:
            tx_hash_seen = False
            retrieved = 0
            unretrievable = []
            for block_num in blocks_to_check:
                block = self.get_block(block_num)
                if not block or 'extrinsics' not in block:
                    unretrievable.append(block_num)
                    continue
                retrieved += 1

                is_raw = block.get('_raw', False)

                for position, ext in enumerate(block['extrinsics']):
                    match = self.match_transfer(ext, tx_hash, is_raw)
                    if match is None:
                        continue

                    tx_hash_seen = True
                    dest, amount, _ = match
                    confs = current_block - block_num
                    if dest != expected_recipient or amount < expected_amount:
                        continue

                    # The call only states intent. Require the Balances.Transfer event before
                    # treating this as a deposit, and take the amount/sender from the event.
                    ext_idx = ext.get('extrinsic_idx', position) if is_raw else position
                    settled = self.settled_credit(block_num, ext_idx, expected_recipient)
                    if settled is None:
                        bt.logging.warning(
                            f'{LOG_SUB} tx {tx_hash[:16]}... is in block {block_num} but moved no funds '
                            f'to {expected_recipient[:8]}... (dispatch failed) — rejecting'
                        )
                        continue
                    settled_sender, settled_amount = settled
                    if settled_amount < expected_amount:
                        bt.logging.warning(
                            f'{LOG_SUB} tx {tx_hash[:16]}... settled {settled_amount} rao to '
                            f'{expected_recipient[:8]}..., below the {expected_amount} required — rejecting'
                        )
                        continue

                    return TransactionInfo(
                        tx_hash=tx_hash,
                        confirmed=confs >= self.get_chain().min_confirmations,
                        sender=settled_sender,
                        recipient=expected_recipient,
                        amount=settled_amount,
                        block_number=block_num,
                        confirmations=confs,
                        block_time=self.get_block_time(block_num),
                    )

            if tx_hash_seen:
                bt.logging.warning(
                    f'{LOG_SUB} scan: tx {tx_hash[:16]}... found but no transfer pays {expected_recipient} >= {expected_amount} rao'
                )
            else:
                # Enriched NOT-FOUND: whether the window was even retrievable tells stale-view apart from
                # a genuinely-absent tx / wrong hint. All-None on a hinted scan == our node couldn't see it.
                summary = (
                    f'{LOG_SUB} scan: tx {tx_hash[:16]}... NOT FOUND — hint={block_hint} head={current_block} '
                    f'retrieved={retrieved}/{len(blocks_to_check)} unretrievable={unretrievable[:8]}'
                )
                if block_hint > 0 and retrieved == 0:
                    bt.logging.warning(
                        summary + ' — ALL hinted blocks unretrievable (stale node view, not an absent tx)'
                    )
                elif block_hint > 0 and not tx_hash_seen and retrieved > 0:
                    bt.logging.warning(
                        summary + ' — hinted blocks retrieved but tx absent from them (hint points at wrong blocks?)'
                    )
                else:
                    bt.logging.debug(summary)
            return None
        except ProviderUnreachableError:
            raise
        except Exception as e:
            raise ProviderUnreachableError(f'TAO block scan failed: {e}') from e

    @staticmethod
    def match_transfer(ext, tx_hash: str, is_raw: bool) -> Optional[Tuple[str, int, str]]:
        """Try to match an extrinsic against a tx hash. Returns (dest, amount, sender) or None."""
        decoded = Tao.decode_transfer(ext, is_raw)
        if decoded is None or decoded[0] != tx_hash:
            return None
        _, dest, amount, sender = decoded
        return dest, amount, sender

    @staticmethod
    def decode_transfer(ext, is_raw: bool) -> Optional[Tuple[str, str, int, str]]:
        """Decode a transfer extrinsic into (tx_hash, dest, amount, sender), or None if it
        isn't a transfer. The single decode shared by the by-hash verifier (match_transfer)
        and the by-content deposit scanner (find_recent_outgoing)."""
        if is_raw:
            ext_hash = ext.get('extrinsic_hash', '')
            if not ext_hash:
                return None
            return ext_hash, ext.get('dest', ''), ext.get('amount', 0), ext.get('sender', '')

        ext_hash = getattr(ext, 'extrinsic_hash', None) or (
            ext.get('extrinsic_hash', '') if isinstance(ext, dict) else ''
        )
        if isinstance(ext_hash, bytes):
            ext_hash = '0x' + ext_hash.hex()
        if not ext_hash:
            return None

        ext_data = ext.value if hasattr(ext, 'value') else ext
        call = ext_data.get('call', {}) if isinstance(ext_data, dict) else {}
        call_function = call.get('call_function', '')
        call_args = call.get('call_args', [])

        if 'transfer' not in call_function.lower():
            return None

        dest = ''
        amount = 0
        sender = ext_data.get('address', '') if isinstance(ext_data, dict) else ''

        for arg in call_args:
            name = arg.get('name', '') if isinstance(arg, dict) else ''
            val = arg.get('value', '') if isinstance(arg, dict) else ''
            if name in ('dest', 'destination'):
                dest = val.get('Id', val) if isinstance(val, dict) else val
            elif name == 'value':
                amount = int(val)

        return ext_hash, dest, amount, sender

    # Substrate has no address index (unlike Esplora / getSignaturesForAddress), so the TAO
    # deposit scanner follows the chain head incrementally: each call scans only the blocks
    # minted since the last call for this (from, to, amount) triple — amortized ~1 block per
    # watcher tick — bounded by SCAN_LOOKBACK_BLOCKS on the first call or after a gap
    # (≈5 min of 12s blocks, mirroring the BTC scanner's window).
    SCAN_LOOKBACK_BLOCKS = 25
    _MAX_SCAN_CURSORS = 64

    def find_recent_outgoing(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        """Extrinsic hash of a recent transfer ``from_addr`` → ``to_addr`` of >= ``amount`` rao,
        else None. The TAO sibling of the BTC/SOL deposit scanners: a hash-finder only — the
        seam's confirm re-verifies everything by hash, so a miss here just means the manual
        rescue paths. An unretrievable block is skipped and not revisited (the cursor moves on)."""
        head = self.get_current_block_height()
        if head is None:
            return None
        key = (from_addr, to_addr, int(amount))
        floor = max(head - self.SCAN_LOOKBACK_BLOCKS, 0)
        last = self.scan_cursors.get(key, floor)
        for block_num in range(max(last, floor) + 1, head + 1):
            block = self.get_block(block_num)
            if not block or 'extrinsics' not in block:
                continue
            is_raw = block.get('_raw', False)
            for position, ext in enumerate(block['extrinsics']):
                decoded = self.decode_transfer(ext, is_raw)
                if decoded is None:
                    continue
                ext_hash, dest, amt, sender = decoded
                if dest != to_addr or sender != from_addr or int(amt) < int(amount):
                    continue
                # Call-level match is only a candidate; confirm the funds actually moved.
                ext_idx = ext.get('extrinsic_idx', position) if is_raw else position
                try:
                    settled = self.settled_credit(block_num, ext_idx, to_addr)
                except ProviderUnreachableError:
                    continue
                if settled is None or settled[1] < int(amount):
                    continue
                self.scan_cursors.pop(key, None)
                return ext_hash
        self.scan_cursors[key] = head
        if len(self.scan_cursors) > self._MAX_SCAN_CURSORS:
            self.scan_cursors.pop(next(iter(self.scan_cursors)))
        return None

    def get_current_block_height(self) -> Optional[int]:
        try:
            return int(self.subtensor.get_current_block())
        except Exception as e:
            bt.logging.debug(f'TAO get_current_block_height failed: {e}')
            return None

    def get_balance(self, address: str) -> int:
        """Get balance for a TAO address in rao."""
        try:
            balance = self.subtensor.get_balance(address)
            return int(balance)
        except Exception as e:
            bt.logging.error(f'TAO get_balance failed: {e}')
            return 0

    def is_valid_address(self, address: str) -> bool:
        """Validate an SS58 address."""
        try:
            if not isinstance(address, str) or len(address) != 48:
                return False
            return is_valid_ss58_address(address)
        except Exception:
            return False

    def sign_from_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        """Sign a message using sr25519 keypair. key should be a Keypair."""
        if key is None or not hasattr(key, 'sign'):
            return ''
        try:
            signature = key.sign(message.encode())
            return signature.hex()
        except Exception as e:
            bt.logging.error(f'TAO sign_from_proof failed: {e}')
            return ''

    def verify_from_proof(self, address: str, message: str, signature: str) -> bool:
        """Verify an sr25519 signature from the given SS58 address."""
        try:
            keypair = Keypair(ss58_address=address)
            sig_bytes = bytes.fromhex(signature[2:] if signature.startswith('0x') else signature)
            return keypair.verify(message.encode(), sig_bytes)
        except Exception as e:
            bt.logging.error(f'TAO verify_from_proof failed: {e}')
            return False

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None
    ) -> Optional[Tuple[str, int]]:
        """Send TAO via subtensor transfer. Amount is in rao."""
        if self.wallet is None:
            bt.logging.error('TAO send_amount called on a read-only Tao (no wallet)')
            return None
        try:
            response = self.subtensor.transfer(
                wallet=self.wallet,
                destination_ss58=to_address,
                amount=bt.Balance.from_rao(amount),
                wait_for_inclusion=True,
                wait_for_finalization=False,
            )
            if not response.success:
                bt.logging.error(f'TAO transfer failed: {response.message}')
                return None
            try:
                receipt = response.extrinsic_receipt
                tx_hash = receipt.extrinsic_hash
                block_num = self.subtensor.substrate.get_block_number(receipt.block_hash)
            except Exception:
                bt.logging.warning('Could not parse transfer receipt, using fallback')
                tx_hash = getattr(getattr(response, 'extrinsic_receipt', None), 'extrinsic_hash', '') or 'tao_transfer'
                block_num = self.subtensor.get_current_block()
            bt.logging.info(f'Sent {amount} rao to {to_address} (tx: {tx_hash}, block: {block_num})')
            return (tx_hash, block_num)
        except Exception as e:
            bt.logging.error(f'TAO transfer error: {e}')
            return None

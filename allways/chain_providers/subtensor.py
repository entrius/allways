import re
from hashlib import blake2b
from typing import Any, Dict, Optional, Tuple

import bittensor as bt
from substrateinterface import Keypair
from substrateinterface.utils.ss58 import ss58_encode

from allways.chain_providers.base import ChainProvider, ProviderUnreachableError, TransactionInfo
from allways.chains import CHAIN_TAO, ChainDefinition


class SubtensorProvider(ChainProvider):
    """TAO chain provider using bt.Subtensor and substrate-interface.

    Owns its signing ``bt.Wallet`` when one is supplied at construction, so
    callers of ``send_amount`` never need to pass key material. Validators
    and read-only consumers can instantiate without a wallet and will get a
    clear error if they attempt to send.
    """

    # Balances pallet index and transfer call indices on Subtensor
    _BALANCES_PALLET = 5
    _TRANSFER_CALLS = {0: 'transfer_allow_death', 3: 'transfer_keep_alive', 7: 'transfer_all'}

    def __init__(self, subtensor: bt.Subtensor, wallet: Optional['bt.Wallet'] = None):
        self.subtensor = subtensor
        self.wallet = wallet
        self.block_cache: Dict[int, dict] = {}

    def get_chain(self) -> ChainDefinition:
        return CHAIN_TAO

    def check_connection(self, **kwargs) -> None:
        try:
            block = self.subtensor.get_current_block()
            bt.logging.success(f'Subtensor connected: block={block}')
        except Exception as e:
            raise ConnectionError(f'Cannot reach Subtensor: {e}') from e

    def clear_cache(self):
        """Clear the block cache. Call at the start of each poll cycle."""
        self.block_cache.clear()

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

            # Sender AccountId is at bytes 1..33
            if len(body) < 33:
                return None
            sender_bytes = body[1:33]
            sender = ss58_encode(sender_bytes, ss58_format=42)

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

    def get_block(self, block_num: int) -> Optional[dict]:
        """Fetch a block, using cache to avoid redundant RPC calls within a poll cycle."""
        if block_num in self.block_cache:
            return self.block_cache[block_num]

        block_hash = self.subtensor.substrate.get_block_hash(block_num)
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

            parsed_exts = []
            for ext_hex in raw_exts:
                parsed = self.parse_raw_extrinsic(ext_hex)
                if parsed:
                    parsed_exts.append(parsed)

            block = {'extrinsics': parsed_exts, '_raw': True}
            self.block_cache[block_num] = block
            return block
        except Exception as e:
            bt.logging.debug(f'Raw block fetch failed for block {block_num}: {e}')
            return None

    def fetch_matching_tx(
        self, tx_hash: str, expected_recipient: str, expected_amount: int, block_hint: int = 0
    ) -> Optional[TransactionInfo]:
        """Scan for a TAO transfer matching recipient + amount.

        If block_hint > 0, checks the hinted block ±3. Otherwise scans the last
        150 blocks. The ±3 window covers small clock/finality skews between the
        caller's block_hint and the block the transfer actually landed in.

        Raises ProviderUnreachableError if subtensor is unreachable.
        """
        try:
            current_block = self.subtensor.get_current_block()
        except Exception as e:
            raise ProviderUnreachableError(f'Subtensor unreachable: {e}') from e

        if block_hint > 0:
            blocks_to_check = [block_hint + offset for offset in range(-3, 4) if block_hint + offset >= 0]
        else:
            blocks_to_check = [current_block - offset for offset in range(150) if current_block - offset >= 0]

        try:
            for block_num in blocks_to_check:
                block = self.get_block(block_num)
                if not block or 'extrinsics' not in block:
                    continue

                is_raw = block.get('_raw', False)

                for ext in block['extrinsics']:
                    match = self.match_transfer(ext, tx_hash, is_raw)
                    if match is None:
                        continue

                    dest, amount, sender = match
                    confs = current_block - block_num
                    if dest == expected_recipient and amount >= expected_amount:
                        return TransactionInfo(
                            tx_hash=tx_hash,
                            confirmed=confs >= self.get_chain().min_confirmations,
                            sender=sender,
                            recipient=dest,
                            amount=amount,
                            block_number=block_num,
                            confirmations=confs,
                        )

            return None
        except ProviderUnreachableError:
            raise
        except Exception as e:
            raise ProviderUnreachableError(f'TAO block scan failed: {e}') from e

    @staticmethod
    def match_transfer(ext, tx_hash: str, is_raw: bool) -> Optional[Tuple[str, int, str]]:
        """Try to match an extrinsic against a tx hash. Returns (dest, amount, sender) or None."""
        if is_raw:
            ext_hash = ext.get('extrinsic_hash', '')
            if ext_hash != tx_hash:
                return None
            return ext.get('dest', ''), ext.get('amount', 0), ext.get('sender', '')

        ext_hash = getattr(ext, 'extrinsic_hash', None) or (
            ext.get('extrinsic_hash', '') if isinstance(ext, dict) else ''
        )
        if isinstance(ext_hash, bytes):
            ext_hash = '0x' + ext_hash.hex()
        if ext_hash != tx_hash:
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

        return dest, amount, sender

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
            if not address or len(address) != 48:
                return False
            return bool(re.match(r'^[1-9A-HJ-NP-Za-km-z]{48}$', address))
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
            sig_bytes = bytes.fromhex(signature)
            return keypair.verify(message.encode(), sig_bytes)
        except Exception as e:
            bt.logging.error(f'TAO verify_from_proof failed: {e}')
            return False

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None
    ) -> Optional[Tuple[str, int]]:
        """Send TAO via subtensor transfer. Amount is in rao."""
        if self.wallet is None:
            bt.logging.error('TAO send_amount called on a read-only SubtensorProvider (no wallet)')
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

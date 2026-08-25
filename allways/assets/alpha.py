from typing import Any, Dict, List, Optional, Tuple

import bittensor as bt

from allways.assets.asset import Asset, ProviderUnreachableError, SendResult, TransactionInfo
from allways.assets.tao import Broadcasts, Decoder, Settler, Tao, Transfer
from allways.chains import ChainDefinition
from allways.constants import CANCEL_REASON_ALPHA_TRANSFER_DISABLED

LOG_ALPHA = '[Alpha]'
# Matched by name: SubtensorModule's indices move on runtime upgrades.
TRANSFER_STAKE = ('SubtensorModule', 'transfer_stake')
SETTLED_EVENTS = {('System', 'ExtrinsicSuccess'), ('SubtensorModule', 'StakeTransferred')}


def event_name(record: Any) -> Optional[Tuple[str, str]]:
    """(pallet, event) of a System.Events record across the shapes scalecodec emits, else None."""
    event = record.get('event', record) if isinstance(record, dict) else None
    if not isinstance(event, dict):
        return None
    module = event.get('module_id') or event.get('module') or event.get('pallet')
    name = event.get('event_id') or event.get('event') or event.get('name')
    if isinstance(module, str) and isinstance(name, str):
        return module, name
    if len(event) == 1:
        ((module, inner),) = event.items()
        if isinstance(module, str) and isinstance(inner, dict) and len(inner) == 1:
            return module, next(iter(inner))
    return None


class Alpha(Asset):
    """A subnet alpha token: transfer_stake on the shared Tao chain, settled by ExtrinsicSuccess + StakeTransferred."""

    def __init__(self, chain_def: ChainDefinition, subtensor: bt.Subtensor, wallet: Optional[bt.Wallet] = None):
        self._chain_def = chain_def
        self._chain = Tao(subtensor, wallet)
        # Per asset, not on the shared chain: a TAO send and an alpha send must never collide.
        self.scan_cursors: Dict[Tuple[str, str, int], int] = {}
        self.broadcasted_txids: Broadcasts = {}

    @property
    def chain_def(self) -> ChainDefinition:
        return self._chain_def

    @property
    def netuid(self) -> int:
        return self._chain_def.netuid

    @property
    def subtensor(self) -> bt.Subtensor:
        return self.chain.subtensor

    @property
    def wallet(self) -> Optional[bt.Wallet]:
        return self.chain.wallet

    def describe(self) -> str:
        return f'{self.chain.describe()} — netuid {self.netuid}'

    def can_send_from(self, address: str) -> bool:
        return self.chain.can_send_from(address)

    def check_connection(self, require_send: bool = True, **kwargs) -> None:
        self.chain.check_connection(**kwargs)
        if require_send and self.wallet is None:
            raise ConnectionError(f'{self.chain_def.id} send requires a wallet')

    def clear_cache(self) -> None:
        self.chain.clear_cache()

    def value_rao(self, amount: int) -> int:
        """Spot value in rao of ``amount`` alpha base units at the pool's current price, floored."""
        try:
            price = self.subtensor.get_subnet_price(self.netuid)
            return int(amount) * int(price.rao) // 10**self.chain_def.decimals
        except Exception as e:
            raise ProviderUnreachableError(f'{self.chain_def.id} price unavailable: {e}') from e

    def decode_transfer_stake(self, ext: Any, is_raw: bool) -> Optional[Transfer]:
        """(hash, dest_coldkey, alpha, sender) of a top-level transfer_stake onto this netuid, else None."""
        if is_raw:
            return None  # the raw fallback only parses Balances transfers
        ext_data = ext.value if hasattr(ext, 'value') else ext
        if not isinstance(ext_data, dict):
            return None
        call = ext_data.get('call') or {}
        if (call.get('call_module'), call.get('call_function')) != TRANSFER_STAKE:
            return None
        args = {a.get('name'): a.get('value') for a in call.get('call_args') or [] if isinstance(a, dict)}
        try:
            if int(args['destination_netuid']) != self.netuid:
                return None
            alpha = int(args['alpha_amount'])
        except (KeyError, TypeError, ValueError):
            return None
        dest = Tao.as_ss58(args.get('destination_coldkey'))
        return Tao.extrinsic_hash(ext), dest, alpha, Tao.as_ss58(ext_data.get('address'))

    def stake_moved(self, block_num: int, extrinsic_idx: int) -> bool:
        """True iff the extrinsic dispatched successfully AND emitted StakeTransferred; raises when unreadable."""
        block_hash = self.chain.get_block_hash(block_num)
        if not block_hash:
            raise ProviderUnreachableError(f'{self.chain_def.id} block hash unavailable for {block_num}')
        events = self.chain.get_block_events(block_hash)
        if not events:
            raise ProviderUnreachableError(f'no events returned for block {block_num}, which holds extrinsics')
        indexed = [(Tao.event_extrinsic_idx(r), event_name(r)) for r in events]
        if all(idx is None for idx, _ in indexed):
            raise ProviderUnreachableError(f'no ApplyExtrinsic phase recognised in {len(events)} events at {block_num}')
        return SETTLED_EVENTS <= {name for idx, name in indexed if idx == extrinsic_idx}

    def settled_transfer_stake(self, block_num: int, ext_idx: int, transfer: Transfer) -> Optional[Tuple[str, int]]:
        """(sender, alpha) from the CALL once settled — the event's amount is the TAO-equivalent."""
        _, _, alpha, sender = transfer
        return (sender, alpha) if self.stake_moved(block_num, ext_idx) else None

    @property
    def ledger(self) -> Tuple[Decoder, Settler]:
        """What the chain's scan mechanics need from this asset: its decoder and its settlement proof."""
        return self.decode_transfer_stake, self.settled_transfer_stake

    def fetch_matching_tx(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        max_scan_blocks: int = 150,
    ) -> Optional[TransactionInfo]:
        info = self.chain.scan_for_tx(
            tx_hash, expected_recipient, expected_amount, block_hint, max_scan_blocks, *self.ledger
        )
        if info is not None and info.block_time is None:
            raise ProviderUnreachableError(f'{self.chain_def.id} block time unavailable for {info.block_number}')
        return info

    def stakes(self, coldkey: str) -> List[Tuple[str, int]]:
        """(hotkey, alpha) held by ``coldkey`` on this netuid; raises on a read failure."""
        infos = self.subtensor.get_stake_info_for_coldkey(coldkey)
        return [(info.hotkey_ss58, int(info.stake.rao)) for info in infos if int(info.netuid) == self.netuid]

    def get_balance(self, address: str) -> int:
        try:
            return sum(alpha for _, alpha in self.stakes(address))
        except Exception as e:
            bt.logging.error(f'{LOG_ALPHA} get_balance failed: {e}')
            return 0

    def transfers_enabled(self) -> bool:
        """TransferToggle ∧ SubtokenEnabled for this netuid; raises on a read failure."""
        flags = (
            self.subtensor.substrate.query('SubtensorModule', name, [self.netuid])
            for name in ('TransferToggle', 'SubtokenEnabled')
        )
        return all(bool(getattr(flag, 'value', flag)) for flag in flags)

    def can_deliver_to(self, address: str, amount: int, from_address: Optional[str] = None) -> bool:
        try:
            return self.transfers_enabled()
        except Exception:
            return True

    def delivery_refused(self, address: str, since_unix: int) -> bool:
        try:
            return not self.transfers_enabled()
        except Exception:
            return False

    def cancel_evidence(
        self, address: str, amount: int, tx_hash: Optional[str] = None, from_address: Optional[str] = None
    ) -> Optional[int]:
        """A subnet that disabled transfers strands every miner on it — no-fault."""
        try:
            return None if self.transfers_enabled() else CANCEL_REASON_ALPHA_TRANSFER_DISABLED
        except Exception:
            return None

    def find_recent_outgoing(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        return self.chain.find_outgoing(self.scan_cursors, from_addr, to_addr, amount, *self.ledger)

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None, dedup_key: Optional[str] = None
    ) -> SendResult:
        """transfer_stake from the hotkey holding the most of this alpha; dedup and hash handling mirror Tao."""
        if self.wallet is None:
            bt.logging.error(f'{LOG_ALPHA} send_amount called on a read-only {self.chain_def.id} (no wallet)')
            return None
        from_ss58 = self.wallet.coldkeypub.ss58_address
        if from_address is not None and from_ss58 != str(from_address):
            bt.logging.error(f'{LOG_ALPHA} committed sender {from_address} != wallet {from_ss58} — not sending')
            return None

        scope = dedup_key or ''
        try:
            landed = self.chain.prior_send_landed(
                self.broadcasted_txids, scope, from_ss58, to_address, amount, *self.ledger
            )
        except Exception as e:
            bt.logging.error(f'{LOG_ALPHA} prior send unresolved ({e}) — not re-sending, would risk a double pay')
            return None
        if landed is not None:
            bt.logging.info(f'{LOG_ALPHA} reusing prior tx {landed[0]} to {to_address} ({amount} alpha)')
            return landed

        try:
            stakes = self.stakes(from_ss58)
        except Exception as e:
            bt.logging.error(f'{LOG_ALPHA} cannot read {from_ss58} stakes: {e} — not sending')
            return None
        if not stakes:
            bt.logging.error(f'{LOG_ALPHA} {from_ss58} holds no netuid {self.netuid} alpha — not sending')
            return None
        hotkey = max(stakes, key=lambda stake: stake[1])[0]

        attempt_head = self.chain.record_send_attempt(self.broadcasted_txids, scope, to_address, amount)
        # The SDK never raises here: every failure comes back as a response, possibly without a hash.
        response = self.subtensor.transfer_stake(
            wallet=self.wallet,
            destination_coldkey_ss58=to_address,
            hotkey_ss58=hotkey,
            origin_netuid=self.netuid,
            destination_netuid=self.netuid,
            amount=bt.Balance.from_rao(int(amount)),
            mev_protection=False,
            wait_for_inclusion=True,
            wait_for_finalization=False,
        )
        # The signed extrinsic exists before broadcast, so its hash outlives a lost receipt.
        receipt = getattr(response, 'extrinsic_receipt', None)
        tx_hash = getattr(receipt, 'extrinsic_hash', None) or Tao.extrinsic_hash(getattr(response, 'extrinsic', None))
        if tx_hash:
            self.broadcasted_txids[scope] = (to_address, int(amount), tx_hash, attempt_head)
        if not response.success:
            bt.logging.error(f'{LOG_ALPHA} transfer_stake failed: {response.message} — recorded, resolved next poll')
            return None
        try:
            block_num = int(self.subtensor.substrate.get_block_number(receipt.block_hash))
        except Exception:
            block_num = attempt_head
        self.broadcasted_txids[scope] = (to_address, int(amount), tx_hash, block_num)
        bt.logging.info(
            f'{LOG_ALPHA} sent {amount} alpha (netuid {self.netuid}) to {to_address} (tx: {tx_hash}, block: {block_num})'
        )
        return (tx_hash, block_num)

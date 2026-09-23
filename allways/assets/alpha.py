from typing import Any, Dict, List, Optional, Tuple

import bittensor as bt

from allways.assets.asset import Asset, ProviderUnreachableError, SendResult, TransactionInfo
from allways.assets.tao import Broadcasts, Decoder, Settler, Tao, Transfer
from allways.chains import ChainDefinition
from allways.constants import CANCEL_REASON_ALPHA_DEST_FULL, CANCEL_REASON_ALPHA_TRANSFER_DISABLED

LOG_ALPHA = '[Alpha]'
# Matched by name: SubtensorModule's indices move on runtime upgrades.
# Both calls change the owning coldkey and share one dispatch path, so both settle 1:1 within a subnet.
TRANSFER_STAKE_CALLS = {
    ('SubtensorModule', 'transfer_stake'),
    ('SubtensorModule', 'transfer_stake_and_hotkey'),
}
EXTRINSIC_SUCCESS = ('System', 'ExtrinsicSuccess')
# One per call — transfer_stake emits the first, transfer_stake_and_hotkey the second.
STAKE_TRANSFER_EVENTS = {
    ('SubtensorModule', 'StakeTransferred'),
    ('SubtensorModule', 'StakeAndHotkeyTransferred'),
}
# u64::MAX is not an amount. Subtensor (spec 469+, move_stake.rs `cap_move_all_to_live_origin`) reads it
# as "the signer's whole live position at execution", so the call names a figure the chain never moved
# and the event carries only the TAO-equivalent. Crediting the call's number would let a dust position
# satisfy any pinned amount. A leg is never credited from it; every allways send names an exact amount.
ALPHA_WHOLE_POSITION = 2**64 - 1
# Mirrors subtensor lib.rs MAX_THIRD_PARTY_STAKING_HOTKEYS (MAX_ROOT_CLAIM_WORK / 2). A transfer to another
# coldkey appends the landing hotkey to that coldkey's StakingHotkeys, and validate_stake_transition refuses
# (TooManyStakingHotkeys) to grow the list past this unless the hotkey is already in it. The recipient
# chooses nothing about a delivery, so the sender must land on a hotkey the recipient can take.
MAX_THIRD_PARTY_STAKING_HOTKEYS = 128


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
        """(hash, dest_coldkey, alpha, sender) of a top-level stake transfer within this netuid, else None."""
        if is_raw:
            # The raw fallback only parses Balances transfers, so it cannot see a stake transfer at all.
            # Returning None here would read as "no such payment" — a slash-eligible verdict on a dest leg.
            raise ProviderUnreachableError(f'{self.chain_def.id} raw block fallback cannot decode a stake transfer')
        ext_data = ext.value if hasattr(ext, 'value') else ext
        if not isinstance(ext_data, dict):
            return None
        call = ext_data.get('call') or {}
        if (call.get('call_module'), call.get('call_function')) not in TRANSFER_STAKE_CALLS:
            return None
        args = {a.get('name'): a.get('value') for a in call.get('call_args') or [] if isinstance(a, dict)}
        try:
            # Both netuids, not just the destination: only a within-subnet transfer moves alpha 1:1. A
            # cross-netuid call unstakes through the AMM and lands a DIFFERENT amount of our alpha, so
            # crediting its origin-denominated alpha_amount would pay out on funds that never arrived.
            if int(args['origin_netuid']) != self.netuid or int(args['destination_netuid']) != self.netuid:
                return None
            alpha = int(args['alpha_amount'])
        except (KeyError, TypeError, ValueError):
            return None
        if alpha >= ALPHA_WHOLE_POSITION:
            bt.logging.debug(f'{LOG_ALPHA} whole-position sentinel in {Tao.extrinsic_hash(ext)[:16]}… — not an amount')
            return None
        # The hotkeys are deliberately unread: the destination coldkey owns the stake whichever hotkey
        # it lands on, so they change who takes a delegate cut, never ownership or the amount.
        dest = Tao.as_ss58(args.get('destination_coldkey'))
        return Tao.extrinsic_hash(ext), dest, alpha, Tao.as_ss58(ext_data.get('address'))

    def stake_moved(self, block_num: int, extrinsic_idx: int) -> bool:
        """True iff the extrinsic dispatched successfully AND emitted its transfer event; raises when unreadable."""
        block_hash = self.chain.get_block_hash(block_num)
        if not block_hash:
            raise ProviderUnreachableError(f'{self.chain_def.id} block hash unavailable for {block_num}')
        events = self.chain.get_block_events(block_hash)
        if not events:
            raise ProviderUnreachableError(f'no events returned for block {block_num}, which holds extrinsics')
        indexed = [(Tao.event_extrinsic_idx(r), event_name(r)) for r in events]
        if all(idx is None for idx, _ in indexed):
            raise ProviderUnreachableError(f'no ApplyExtrinsic phase recognised in {len(events)} events at {block_num}')
        names = {name for idx, name in indexed if idx == extrinsic_idx}
        return EXTRINSIC_SUCCESS in names and bool(names & STAKE_TRANSFER_EVENTS)

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
        try:
            infos = self.subtensor.get_stake_info_for_coldkey(coldkey)
        except Exception as e:
            raise ProviderUnreachableError(f'{self.chain_def.id} stake unavailable for {coldkey}: {e}') from e
        return [(info.hotkey_ss58, int(info.stake.rao)) for info in infos if int(info.netuid) == self.netuid]

    def get_balance(self, address: str) -> int:
        """Alpha held on this netuid across every hotkey; raises when the read fails — see Tao's."""
        return sum(alpha for _, alpha in self.stakes(address))

    def staking_hotkeys(self, coldkey: str) -> List[str]:
        """The hotkeys ``coldkey`` stakes to (any netuid) — SubtensorModule::StakingHotkeys; raises when unreadable."""
        try:
            value = self.subtensor.substrate.query('SubtensorModule', 'StakingHotkeys', [coldkey])
        except Exception as e:
            raise ProviderUnreachableError(f'{self.chain_def.id} StakingHotkeys unavailable for {coldkey}: {e}') from e
        return [Tao.as_ss58(hotkey) for hotkey in (getattr(value, 'value', value) or [])]

    def landing_hotkey(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        """The hotkey a delivery of ``amount`` from ``from_addr`` can land on at ``to_addr``: one the sender
        holds enough on that the recipient already stakes to (never grows the recipient's list), else the
        sender's largest sufficient one while the recipient's list can still grow. None when no single
        hotkey holds ``amount`` (a summed balance is not a sendable one) or the recipient is at the cap
        with nothing in common. Raises when unreadable."""
        held = [(hotkey, alpha) for hotkey, alpha in self.stakes(from_addr) if alpha >= amount]
        if not held:
            return None
        recipient = set(self.staking_hotkeys(to_addr))
        shared = [stake for stake in held if stake[0] in recipient]
        if shared:
            return max(shared, key=lambda stake: stake[1])[0]
        if len(recipient) >= MAX_THIRD_PARTY_STAKING_HOTKEYS:
            return None
        return max(held, key=lambda stake: stake[1])[0]

    def recipient_full(self, to_addr: str, from_addr: str) -> bool:
        """Positive evidence ``to_addr`` can take no delivery from ``from_addr`` at all: its StakingHotkeys
        is at the cap and the sender holds this alpha on none of them. Raises when unreadable."""
        recipient = set(self.staking_hotkeys(to_addr))
        if len(recipient) < MAX_THIRD_PARTY_STAKING_HOTKEYS:
            return False
        return not any(hotkey in recipient for hotkey, alpha in self.stakes(from_addr) if alpha > 0)

    def subnet_flag(self, name: str) -> bool:
        """One SubtensorModule per-netuid flag, read live; raises ProviderUnreachableError on a read failure."""
        try:
            flag = self.subtensor.substrate.query('SubtensorModule', name, [self.netuid])
        except Exception as e:
            raise ProviderUnreachableError(f'{self.chain_def.id} {name} unavailable: {e}') from e
        return bool(getattr(flag, 'value', flag))

    def transfers_enabled(self) -> bool:
        """Whether this alpha can move at all: the subnet exists, its token was started, and
        transfers are on. Raises on a read failure.

        NetworksAdded covers the prune case — SubnetLimit is full, so registering a subnet
        dissolves the lowest-priced one, and its alpha is force-liquidated to coldkey TAO.
        SubtokenEnabled is false until the owner calls start_call, and never returns to false.
        TransferToggle is flippable by the owner (or root) at any block, so it is re-read here
        rather than cached — it is the only one of the three that can turn off mid-swap."""
        return all(self.subnet_flag(name) for name in ('NetworksAdded', 'SubtokenEnabled', 'TransferToggle'))

    def can_deliver_to(self, address: str, amount: int, from_address: Optional[str] = None) -> bool:
        try:
            if not self.transfers_enabled():
                return False
            return from_address is None or not self.recipient_full(address, from_address)
        except Exception:
            return True

    def delivery_refused(self, address: str, since_unix: int) -> bool:
        """Deferral hint: transfers are off right now. Raises when unreadable, so the loop defers rather
        than reading an RPC failure as "not refused" and slashing on it."""
        return not self.transfers_enabled()

    def cancel_evidence(
        self, address: str, amount: int, tx_hash: Optional[str] = None, from_address: Optional[str] = None
    ) -> Optional[int]:
        """No-fault only when the subnet itself is gone (pruned: its alpha was force-liquidated to TAO, so
        there is nothing left to deliver). A TransferToggle flip is deliberately NOT cancel evidence: the
        toggle is the subnet owner's to flip at any block, a cancel leaves the taker's deposit with the
        miner, and an owner running a miner on its own alpha could flip it after every deposit. It stays
        a deferral (`delivery_refused`) — the swap holds while transfers are off and, like an EVM
        getCode hint, times out at the extension ceiling if they never return. A subnet whose owner
        disables transfers is the miner's counterparty risk for quoting it."""
        try:
            if not self.subnet_flag('NetworksAdded'):
                return CANCEL_REASON_ALPHA_TRANSFER_DISABLED
            if from_address is not None and self.recipient_full(address, from_address):
                return CANCEL_REASON_ALPHA_DEST_FULL
            return None
        except Exception:
            return None

    def find_recent_outgoing(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        return self.chain.find_outgoing(self.scan_cursors, from_addr, to_addr, amount, *self.ledger)

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None, dedup_key: Optional[str] = None
    ) -> SendResult:
        """transfer_stake from a hotkey the recipient can take (``landing_hotkey``); dedup and hash handling mirror Tao."""
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
            hotkey = self.landing_hotkey(from_ss58, to_address, amount)
        except Exception as e:
            bt.logging.error(f'{LOG_ALPHA} cannot pick a landing hotkey for {to_address}: {e} — not sending')
            return None
        if hotkey is None:
            # Either no single hotkey holds the amount (the chain debits ONE position, a summed balance is
            # not sendable) or the recipient is at its StakingHotkeys cap with nothing in common — a
            # transfer_stake would dispatch and fail, paying a fee per poll until the swap timed out.
            bt.logging.error(
                f'{LOG_ALPHA} no hotkey of {from_ss58} can land {amount} netuid-{self.netuid} alpha at '
                f'{to_address} (single-hotkey inventory, or recipient at the StakingHotkeys cap) — not sending'
            )
            return None

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
        if not response.success or not tx_hash:
            bt.logging.error(
                f'{LOG_ALPHA} transfer_stake unresolved: {response.message} — recorded, resolved next poll'
            )
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

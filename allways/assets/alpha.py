from typing import Any, Dict, List, Optional, Tuple

import bittensor as bt

from allways.assets.asset import Asset, ProviderUnreachableError, SendResult, TransactionInfo
from allways.assets.tao import Broadcasts, Decoder, Settler, Tao, Transfer
from allways.chains import ChainDefinition
from allways.constants import CANCEL_REASON_ALPHA_STAKE_REFUSED

LOG_ALPHA = '[Alpha]'
# Matched by name: SubtensorModule's indices move on runtime upgrades.
# Both calls change the owning coldkey and share one dispatch path, so both settle 1:1 within a subnet.
TRANSFER_STAKE_CALLS = {
    ('SubtensorModule', 'transfer_stake'),
    ('SubtensorModule', 'transfer_stake_and_hotkey'),
}
EXTRINSIC_SUCCESS = ('System', 'ExtrinsicSuccess')
EXTRINSIC_FAILED = ('System', 'ExtrinsicFailed')
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
# Mirrors subtensor lib.rs ACCOUNT_FLAGS_REJECT_STAKING_HOTKEY_GROWTH: the recipient takes only hotkeys it
# already stakes to or owns, failing any other with this module error.
REJECT_STAKING_HOTKEY_GROWTH = 1 << 1
STAKE_HOTKEY_REFUSED = 'AccountRejectsStakingHotkeyGrowth'
# Emitted only by set_reject_staking_hotkey_growth, signed by the coldkey itself. A coldkey swap also copies
# AccountFlags onto its (empty) new coldkey but emits no such event, so this names who turned the flag on.
FLAG_SET_EVENT = ('SubtensorModule', 'RejectStakingHotkeyGrowthUpdated')


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

    def value_rao(self, amount: int, block: Optional[int] = None) -> int:
        """Value in rao of ``amount`` alpha base units at ``block`` (or head), floored."""
        try:
            price = self.subtensor.get_subnet_price(self.netuid, block=block)
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
        return self.chain.scan_for_tx(
            tx_hash, expected_recipient, expected_amount, block_hint, max_scan_blocks, *self.ledger
        )

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

    def account_storage(self, name: str, coldkey: str, block_hash: Optional[str] = None) -> Any:
        """One SubtensorModule per-coldkey storage value; raises when unreadable."""
        try:
            value = self.subtensor.substrate.query('SubtensorModule', name, [coldkey], block_hash=block_hash)
        except Exception as e:
            raise ProviderUnreachableError(f'{self.chain_def.id} {name} unavailable for {coldkey}: {e}') from e
        return getattr(value, 'value', value)

    def staking_hotkeys(self, coldkey: str, block_hash: Optional[str] = None) -> List[str]:
        """The hotkeys ``coldkey`` stakes to (any netuid) — SubtensorModule::StakingHotkeys; raises when unreadable."""
        return [Tao.as_ss58(hotkey) for hotkey in self.account_storage('StakingHotkeys', coldkey, block_hash) or []]

    def owned_hotkeys(self, coldkey: str, block_hash: Optional[str] = None) -> List[str]:
        """The hotkeys ``coldkey`` owns — SubtensorModule::OwnedHotkeys; raises when unreadable."""
        return [Tao.as_ss58(hotkey) for hotkey in self.account_storage('OwnedHotkeys', coldkey, block_hash) or []]

    def rejects_new_hotkeys(self, coldkey: str, block_hash: Optional[str] = None) -> bool:
        """Whether ``coldkey`` refuses stake on hotkeys it neither stakes to nor owns; raises when unreadable."""
        flags = self.account_storage('AccountFlags', coldkey, block_hash)
        return bool(int(flags or 0) & REJECT_STAKING_HOTKEY_GROWTH)

    def landing_hotkeys(self, from_addr: str, to_addr: str, amount: int) -> Optional[Tuple[str, str]]:
        """(origin, destination) hotkeys for a delivery of ``amount``: the sender's position holding all of it
        (one the recipient already stakes to first), landing on that same hotkey while the recipient's
        StakingHotkeys can take it, else on a hotkey the recipient already stakes to or owns — a full list or
        a refuse-new-hotkey flag never blocks a delivery that has one. None when no single hotkey holds
        ``amount``. Raises when unreadable."""
        held = [(hotkey, alpha) for hotkey, alpha in self.stakes(from_addr) if alpha >= amount]
        if not held:
            return None
        recipient = self.staking_hotkeys(to_addr)
        shared = [stake for stake in held if stake[0] in recipient]
        origin = max(shared or held, key=lambda stake: stake[1])[0]
        if origin in recipient or (
            len(recipient) < MAX_THIRD_PARTY_STAKING_HOTKEYS and not self.rejects_new_hotkeys(to_addr)
        ):
            return origin, origin
        # No hotkey to land on: the send fails on-chain and that failure is the refusal evidence.
        landing = recipient or self.owned_hotkeys(to_addr) or [origin]
        return origin, landing[0]

    def free_to_send(self, coldkey: str) -> int:
        """Alpha ``coldkey`` can move on this netuid: stake less its conviction lock and miner collateral
        (subtensor's own availability). Raises when unreadable."""
        try:
            value = self.subtensor.substrate.runtime_call(
                'StakeInfoRuntimeApi', 'get_stake_availability_for_coldkeys', [[coldkey], [self.netuid]]
            )
            by_netuid = getattr(value, 'value', value).get(coldkey, {})
            return int(by_netuid[self.netuid]['available']) if self.netuid in by_netuid else 0
        except Exception as e:
            raise ProviderUnreachableError(f'{self.chain_def.id} stake availability unavailable: {e}') from e

    def send_blocker(self, from_address: str, to_address: str, amount: int) -> Optional[str]:
        """A transfer_stake debits ONE hotkey position and cannot move locked alpha; fails open when unreadable."""
        try:
            largest = max((alpha for _, alpha in self.stakes(from_address)), default=0)
            free = self.free_to_send(from_address) if largest >= amount else amount
        except ProviderUnreachableError:
            return None
        name, scale = self.chain_def.id.upper(), 10**self.chain_def.decimals
        if largest < amount:
            return (
                f'{name} must go out as one transfer_stake from one hotkey; your largest position holds '
                f'{largest / scale:.9g} of the {amount / scale:.9g} needed — move it onto one hotkey first'
            )
        if free < amount:
            return (
                f'only {free / scale:.9g} of your {name} is free to send (the rest is locked); swap that much or less'
            )
        return None

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
            return self.transfers_enabled()
        except Exception:
            return True

    def delivery_refused(self, address: str, since_unix: int) -> bool:
        """Deferral hint, never cancel evidence (a cancel leaves the taker's deposit with the miner).
        Raises when unreadable so the loop defers, not slashes."""
        return not self.transfers_enabled()

    def dispatch_error(self, block_num: int, extrinsic_idx: int) -> Optional[str]:
        """Name of the module error that failed this extrinsic, else None; raises when unreadable."""
        block_hash = self.chain.get_block_hash(block_num)
        if not block_hash:
            raise ProviderUnreachableError(f'{self.chain_def.id} block hash unavailable for {block_num}')
        for record in self.chain.get_block_events(block_hash):
            if Tao.event_extrinsic_idx(record) != extrinsic_idx or event_name(record) != EXTRINSIC_FAILED:
                continue
            attributes = record.get('event', record).get('attributes') or {}
            error = (attributes.get('dispatch_error') or {}).get('Module') if isinstance(attributes, dict) else None
            if not isinstance(error, dict):
                return None
            code = error.get('error')
            index = int(code[2:4], 16) if isinstance(code, str) else (code[0] if code else 0)
            metadata = self.subtensor.substrate.metadata
            return metadata.get_module_error(module_index=int(error['index']), error_index=index).name
        return None

    @staticmethod
    def landing_hotkey(ext: Any) -> str:
        """The hotkey a stake transfer lands on: ``hotkey`` for transfer_stake, ``destination_hotkey`` for
        transfer_stake_and_hotkey; '' when unreadable."""
        call = (ext.value if hasattr(ext, 'value') else ext).get('call') or {}
        args = {a.get('name'): a.get('value') for a in call.get('call_args') or [] if isinstance(a, dict)}
        return Tao.as_ss58(args.get('destination_hotkey', args.get('hotkey')))

    def flag_turned_on_by(self, block_num: int, before_idx: int, coldkey: str) -> bool:
        """Whether ``coldkey`` itself turned its refuse-new-hotkey flag on in an extrinsic of ``block_num``
        ahead of ``before_idx``; raises when unreadable."""
        block_hash = self.chain.get_block_hash(block_num)
        if not block_hash:
            raise ProviderUnreachableError(f'{self.chain_def.id} block hash unavailable for {block_num}')
        for record in self.chain.get_block_events(block_hash):
            idx = Tao.event_extrinsic_idx(record)
            if idx is None or idx >= before_idx or event_name(record) != FLAG_SET_EVENT:
                continue
            attributes = record.get('event', record).get('attributes')
            if isinstance(attributes, dict):
                who, enabled = attributes.get('coldkey'), attributes.get('enabled')
            elif isinstance(attributes, (list, tuple)) and len(attributes) == 2:
                who, enabled = attributes
            else:
                continue
            if enabled is True and Tao.as_ss58(who) == coldkey:
                return True
        return False

    def cancel_evidence(
        self,
        address: str,
        amount: int,
        tx_hash: Optional[str] = None,
        from_address: Optional[str] = None,
        block_hint: int = 0,
    ) -> Optional[int]:
        """No-fault cancel: the committed miner's own transfer_stake of the full amount to ``address`` failed
        on its refuse-new-hotkey flag, and the miner could not have avoided it. That is either (a) at the
        parent block the recipient neither staked to nor owned any hotkey, so no delivery could land, or
        (b) at the parent block the flag was off and the landing hotkey would have been taken, and the
        recipient itself turned the flag on earlier in the inclusion block. Every read is pinned to those
        two blocks, so every validator reaches the same verdict. A recipient that was payable at the parent
        block and did not change its own state is never refused. Raises when unreadable."""
        if not tx_hash or block_hint <= 0:
            return None
        for position, ext in enumerate(self.chain.decoded_extrinsics(block_hint)):
            transfer = self.decode_transfer_stake(ext, False)
            if transfer is None or transfer[0] != tx_hash:
                continue
            _, dest, alpha, sender = transfer
            if dest != Tao.as_ss58(address) or alpha < amount:
                return None
            if from_address and sender != Tao.as_ss58(from_address):
                return None
            if self.dispatch_error(block_hint, position) != STAKE_HOTKEY_REFUSED:
                return None
            parent = self.chain.get_block_hash(block_hint - 1)
            if not parent:
                # A None hash reads head state, not the parent block's.
                raise ProviderUnreachableError(f'{self.chain_def.id} block hash unavailable for {block_hint - 1}')
            staking = self.staking_hotkeys(dest, parent)
            if not staking and not self.owned_hotkeys(dest, parent):
                return CANCEL_REASON_ALPHA_STAKE_REFUSED
            if self.rejects_new_hotkeys(dest, parent):
                return None
            landing = self.landing_hotkey(ext)
            if landing not in staking and len(staking) >= MAX_THIRD_PARTY_STAKING_HOTKEYS:
                return None
            if not self.flag_turned_on_by(block_hint, position, dest):
                return None
            return CANCEL_REASON_ALPHA_STAKE_REFUSED
        return None

    def transfer(self, to_address: str, origin_hotkey: str, destination_hotkey: str, amount: int) -> Any:
        """A plain (never MEV-shielded) transfer_stake, or transfer_stake_and_hotkey to land on a different hotkey."""
        if origin_hotkey == destination_hotkey:
            return self.subtensor.transfer_stake(
                wallet=self.wallet,
                destination_coldkey_ss58=to_address,
                hotkey_ss58=origin_hotkey,
                origin_netuid=self.netuid,
                destination_netuid=self.netuid,
                amount=bt.Balance.from_rao(int(amount)),
                mev_protection=False,
                wait_for_inclusion=True,
                wait_for_finalization=False,
            )
        call = self.subtensor.compose_call(
            'SubtensorModule',
            'transfer_stake_and_hotkey',
            {
                'destination_coldkey': to_address,
                'origin_hotkey': origin_hotkey,
                'destination_hotkey': destination_hotkey,
                'origin_netuid': self.netuid,
                'destination_netuid': self.netuid,
                'alpha_amount': int(amount),
            },
        )
        return self.subtensor.sign_and_send_extrinsic(
            call=call, wallet=self.wallet, wait_for_inclusion=True, wait_for_finalization=False
        )

    def find_recent_outgoing(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        return self.chain.find_outgoing(self.scan_cursors, from_addr, to_addr, amount, *self.ledger)

    def locate_transfer(self, block_num: int, ext_idx: int) -> Tuple[str, int]:
        return self.chain.locate_transfer(block_num, ext_idx, self.decode_transfer_stake)

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None, dedup_key: Optional[str] = None
    ) -> SendResult:
        """Deliver from one sufficient hotkey (``landing_hotkeys``); dedup and hash handling mirror Tao."""
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
            hotkeys = self.landing_hotkeys(from_ss58, to_address, amount)
        except Exception as e:
            bt.logging.error(f'{LOG_ALPHA} cannot pick a landing hotkey for {to_address}: {e} — not sending')
            return None
        if hotkeys is None:
            # The chain debits ONE position: a summed balance is not sendable, and a transfer_stake would
            # dispatch and fail, paying a fee per poll until the swap timed out.
            bt.logging.error(
                f'{LOG_ALPHA} no single hotkey of {from_ss58} holds {amount} netuid-{self.netuid} alpha — not sending. '
                'A delivery is ONE transfer from ONE hotkey: keep this alpha on a single hotkey (btcli stake move)'
            )
            return None

        for resend in (False, True):
            attempt_head = self.chain.record_send_attempt(self.broadcasted_txids, scope, to_address, amount)
            # The SDK never raises here: every failure comes back as a response, possibly without a hash.
            response = self.transfer(to_address, *hotkeys, amount)
            # The signed extrinsic exists before broadcast, so its hash outlives a lost receipt.
            receipt = getattr(response, 'extrinsic_receipt', None)
            tx_hash = getattr(receipt, 'extrinsic_hash', None) or Tao.extrinsic_hash(
                getattr(response, 'extrinsic', None)
            )
            if tx_hash:
                self.broadcasted_txids[scope] = (to_address, int(amount), tx_hash, attempt_head)
            refused = not response.success and STAKE_HOTKEY_REFUSED in str(response.message)
            if (not response.success and not refused) or not tx_hash:
                bt.logging.error(
                    f'{LOG_ALPHA} transfer_stake unresolved: {response.message} — recorded, resolved next poll'
                )
                return None
            try:
                block_num = int(self.subtensor.substrate.get_block_number(receipt.block_hash))
            except Exception:
                block_num = attempt_head
            self.broadcasted_txids[scope] = (to_address, int(amount), tx_hash, block_num)
            if not refused:
                bt.logging.info(
                    f'{LOG_ALPHA} sent {amount} alpha (netuid {self.netuid}) to {to_address} '
                    f'(tx: {tx_hash}, block: {block_num})'
                )
                return (tx_hash, block_num)
            # The recipient changed its flag or list after the pick. The failed tx moved nothing, so
            # re-pick from the state that refused it and pay onto a hotkey it takes, once.
            retry = None if resend or receipt is None else self.repick(from_ss58, to_address, amount)
            if retry is None or retry[1] == hotkeys[1]:
                break
            bt.logging.warning(
                f'{LOG_ALPHA} {to_address} refused stake on {hotkeys[1]} (failed tx {tx_hash}) — '
                f'resending onto {retry[1]}'
            )
            hotkeys = retry
        # Marked fulfilled like a delivery: validators read the failed tx as no-fault refusal evidence.
        bt.logging.warning(
            f'{LOG_ALPHA} {to_address} refuses new stake hotkeys — failed tx {tx_hash} (block {block_num}) '
            'submitted as refusal evidence'
        )
        return (tx_hash, block_num)

    def repick(self, from_addr: str, to_addr: str, amount: int) -> Optional[Tuple[str, str]]:
        """``landing_hotkeys`` re-read after a refusal, None when unreadable."""
        try:
            return self.landing_hotkeys(from_addr, to_addr, amount)
        except Exception as e:
            bt.logging.warning(f'{LOG_ALPHA} cannot re-pick a landing hotkey for {to_addr}: {e}')
            return None

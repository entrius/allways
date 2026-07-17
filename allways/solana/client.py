"""AllwaysSolanaClient — read PDAs, build/send instructions, ingest events for allways_swap_manager.

Hand-rolled + sync. The validator's sole on-chain client (the old ink!/Substrate client is gone). B0 ships all account readers +
getProgramAccounts discovery + the tx build/sign/send pipeline + a representative write set (bind_hotkey,
set_quote, post/withdraw_collateral) + an event-log ingest skeleton. The remaining ~25 instruction
builders land in B1/B2 as the loop needs them.
"""

import base64
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

from Crypto.Hash import keccak
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import Transaction

from allways.solana import layouts, pdas
from allways.solana.program import resolve_program_id
from allways.solana.rpc import SolanaRpc

SYSTEM_PROGRAM = Pubkey.from_string('11111111111111111111111111111111')
SLOT_HASHES = Pubkey.from_string('SysvarS1otHashes111111111111111111111111111')  # RNG seed source for resolve_pool


def swap_key_from_tx_hash(from_tx_hash: str) -> bytes:
    """swap_key = keccak256(from_tx_hash) — matches the contract's hashv(&[from_tx_hash.as_bytes()])."""
    return keccak.new(data=from_tx_hash.encode(), digest_bits=256).digest()


def weights_round_key(validator_keys: List[bytes], weights: List[int]) -> bytes:
    """Mirror of the contract's consensus::weights_hash — keccak256 over
    REQ_SET_WEIGHTS || each validator key (config order) || each weight LE. Binds a weights vote to
    its exact snapshot and doubles as the per-snapshot vote-round PDA key."""
    h = keccak.new(digest_bits=256)
    h.update(bytes([pdas.REQ_SET_WEIGHTS]))
    for k in validator_keys:
        h.update(bytes(k))
    for w in weights:
        h.update(int(w).to_bytes(8, 'little'))
    return h.digest()


@dataclass
class SolanaSwap:
    """Miner-facing view of an on-chain `Swap`, keyed by its `swap_key` (== keccak(from_tx_hash)).

    Flattens the borsh `Swap` layout into the fields the miner poller/fulfiller consume. Replaces the
    ink! `classes.Swap` (int id → swap_key bytes; block `timeout_block` → unix `timeout_at`; ss58
    `miner_hotkey` → `miner` pubkey; no per-swap fee — `to_amount` is the full pinned payout).
    """

    swap_key: bytes
    miner: Pubkey
    user: Pubkey
    from_chain: str
    to_chain: str
    user_from_addr: str
    user_to_addr: str
    miner_from_addr: str
    miner_to_addr: str
    rate: int  # u128 fixed-point, as stored on-chain
    collateral_amount: int
    from_amount: int
    to_amount: int
    from_tx_hash: str
    from_tx_block: int
    to_tx_hash: str
    to_tx_block: int
    status: str  # 'Active' | 'Fulfilled' | 'PendingAttestation'
    initiated_at: int
    timeout_at: int
    max_extend_at: int
    fulfilled_at: int

    @property
    def key_hex(self) -> str:
        return self.swap_key.hex()


def swap_from_solana(acct, swap_key: Optional[bytes] = None) -> SolanaSwap:
    """Adapt a decoded `Swap` account into a `SolanaSwap`. `swap_key` is derived from `from_tx_hash`
    when not supplied (the swap_key is the PDA seed, not stored in the account)."""
    if swap_key is None:
        swap_key = swap_key_from_tx_hash(acct.from_tx_hash)
    return SolanaSwap(
        swap_key=swap_key,
        miner=_as_pubkey(acct.miner),
        user=_as_pubkey(acct.user),
        from_chain=acct.from_chain,
        to_chain=acct.to_chain,
        user_from_addr=acct.user_from_addr,
        user_to_addr=acct.user_to_addr,
        miner_from_addr=acct.miner_from_addr,
        miner_to_addr=acct.miner_to_addr,
        rate=acct.rate,  # u128 fixed-point, as stored on-chain; calculate_to_amount takes the int directly
        collateral_amount=acct.collateral_amount,
        from_amount=acct.from_amount,
        to_amount=acct.to_amount,
        from_tx_hash=acct.from_tx_hash,
        from_tx_block=acct.from_tx_block,
        to_tx_hash=acct.to_tx_hash,
        to_tx_block=acct.to_tx_block,
        status=type(acct.status).__name__,
        initiated_at=acct.initiated_at,
        timeout_at=acct.timeout_at,
        max_extend_at=acct.max_extend_at,
        fulfilled_at=acct.fulfilled_at,
    )


def _as_pubkey(p) -> Pubkey:
    if isinstance(p, Pubkey):
        return p
    if isinstance(p, (bytes, bytearray)):
        return Pubkey.from_bytes(bytes(p))
    return Pubkey.from_string(str(p))


class SolanaClientError(Exception):
    pass


def contract_reject_reason(err: Exception) -> Optional[str]:
    """A deliberate on-chain program rejection is a normal domain rejection — e.g. the miner got
    reserved between a pre-check and this tx (a race the contract, as final arbiter, closes).
    Returns a clean human reason, or None for a genuine transport/RPC fault which must still
    surface as an error.

    A program rejection surfaces two ways: a PRE-FLIGHT simulation reject carries the Anchor name /
    'custom program error' text; a tx that is submitted and LANDS failed surfaces through the confirm
    path as `{'InstructionError': [0, {'Custom': N}]}` — a numeric code with neither phrase."""
    s = str(err)
    sl = s.lower()
    if not ('custom program error' in sl or 'anchorerror' in sl or 'instructionerror' in sl or "'custom':" in sl):
        return None
    m = re.search(r'Error Message: ([^.\"\']+)', s)
    if m:
        return m.group(1).strip()
    code = re.search(r"'Custom':\s*(\d+)", s)  # landed-tx form has no human message — surface the code
    if code:
        return f'miner is not available for reservation right now (contract error {code.group(1)})'
    return 'miner is not available for reservation right now'


class AllwaysSolanaClient:
    def __init__(
        self,
        rpc_url: str,
        program_id: Optional[Pubkey] = None,
        keypair: Optional[Keypair] = None,
        timeout: int = 30,
    ):
        self.rpc = SolanaRpc(rpc_url, timeout=timeout)
        self.program_id = program_id or resolve_program_id()
        self.keypair = keypair

    # ---------- decode ----------
    def _decode(self, name: str, raw: bytes):
        disc = layouts.DISCRIMINATORS[name]
        if raw[:8] != disc:
            raise SolanaClientError(f'{name}: account discriminator mismatch')
        c = layouts.ACCOUNT_LAYOUTS[name].parse(raw[8:])
        for f in layouts.ACCOUNT_PUBKEY_FIELDS.get(name, []):
            c[f] = Pubkey.from_bytes(bytes(c[f]))
        return c

    def _get(self, name: str, pubkey: Pubkey):
        raw = self.rpc.get_account_info(pubkey)
        return None if raw is None else self._decode(name, raw)

    # ---------- readers ----------
    def get_config(self):
        return self._get('Config', pdas.config_pda(self.program_id))

    def get_treasury(self):
        return self._get('Treasury', pdas.treasury_pda(self.program_id))

    def get_miner_state(self, miner):
        return self._get('MinerState', pdas.miner_state_pda(miner, self.program_id))

    def get_binding(self, miner):
        return self._get('Binding', pdas.binding_pda(miner, self.program_id))

    def get_hotkey_binding(self, hotkey: bytes):
        return self._get('HotkeyBinding', pdas.hotkey_binding_pda(hotkey, self.program_id))

    def get_reservation(self, miner):
        return self._get('Reservation', pdas.reservation_pda(miner, self.program_id))

    def get_pool(self, miner):
        return self._get('Pool', pdas.pool_pda(miner, self.program_id))

    def get_swap(self, swap_key: bytes):
        return self._get('Swap', pdas.swap_pda(swap_key, self.program_id))

    def get_quote(self, miner, from_chain: str, to_chain: str):
        return self._get('MinerQuote', pdas.quote_pda(miner, from_chain, to_chain, self.program_id))

    def get_direction_stats(self, miner, from_chain: str, to_chain: str):
        return self._get('MinerDirectionStats', pdas.stats_pda(miner, from_chain, to_chain, self.program_id))

    def get_collateral_lamports(self, miner) -> Optional[int]:
        """Slashable/gating collateral = the tracked ``MinerState.collateral`` — the exact field the
        contract checks in finalize_reservation / vote_initiate. NOT the vault's raw lamports, which
        include the account's rent-exempt reserve (~0.00089 SOL) and would over-credit capacity and
        viability everywhere this is read. None if the miner never posted collateral."""
        ms = self.get_miner_state(miner)
        return int(ms.collateral) if ms is not None else None

    def get_vote_round(self, req_type: int, target=None):
        return self._get('VoteRound', pdas.vote_round_pda(req_type, target, self.program_id))

    def has_voted(self, req_type: int, target, voter) -> bool:
        """True if `voter` (Pubkey/bytes) already recorded a vote in this round — skip a wasted re-vote."""
        vr = self.get_vote_round(req_type, target)
        if vr is None:
            return False
        vb = bytes(_as_pubkey(voter))
        return any(bytes(v) == vb for v in vr.voters)

    # ---------- discovery (getProgramAccounts by discriminator) ----------
    def get_all(self, name: str) -> List[Tuple[str, object]]:
        out = []
        for pubkey, raw in self.rpc.get_program_accounts(self.program_id, disc8=layouts.DISCRIMINATORS[name]):
            out.append((pubkey, self._decode(name, raw)))
        return out

    def get_swaps(self, status: Optional[str] = None) -> List[Tuple[str, object]]:
        swaps = self.get_all('Swap')
        if status is None:
            return swaps
        return [(p, s) for p, s in swaps if type(s.status).__name__ == status]

    # ---------- tx pipeline ----------
    def _send(
        self,
        instructions: List[Instruction],
        signers: Optional[List[Keypair]] = None,
        skip_preflight: bool = False,
        retries: int = 8,
    ) -> str:
        if self.keypair is None:
            raise SolanaClientError('client has no keypair; cannot send transactions')
        signers = signers or [self.keypair]
        last_err: Optional[Exception] = None
        for attempt in range(retries):
            blockhash = Hash.from_string(self.rpc.get_latest_blockhash())
            tx = Transaction.new_signed_with_payer(instructions, self.keypair.pubkey(), signers, blockhash)
            try:
                sig = self.rpc.send_transaction(base64.b64encode(bytes(tx)).decode(), skip_preflight=skip_preflight)
                self.rpc.confirm(sig)
                return sig
            except Exception as e:  # transient stale-blockhash on a fresh/lagging RPC → re-fetch + resend
                msg = str(e).lower()
                # Match the actual staleness signature only — NOT the substring 'blockhash', which also
                # appears in the `replacementBlockhash` field of every program-error payload (that would
                # retry a deterministic contract rejection and bury it as a "blockhash retries" error).
                # A BlockhashNotFound tx never entered the ledger, so re-signing with a FRESH blockhash
                # can't double-submit — safe to retry hard. Exponential backoff (~0.5→4s, ~23s total)
                # rides out a sustained degraded-RPC window that the old 5×0.5s (~2.5s) blew straight
                # through, orphaning a paid-for seat mid-origination.
                if 'blockhash not found' not in msg and 'block height exceeded' not in msg:
                    raise
                last_err = e
                time.sleep(min(4.0, 0.5 * 2**attempt))
        raise SolanaClientError(f'send failed after {retries} blockhash retries: {last_err}')

    def _ix(self, name: str, arg_bytes: bytes, metas: List[AccountMeta]) -> Instruction:
        return Instruction(self.program_id, layouts.IX_DISCRIMINATORS[name] + arg_bytes, metas)

    # ---------- bootstrap (admin) ----------
    def initialize(
        self,
        min_collateral: int,
        max_collateral: int,
        fulfillment_timeout_secs: int,
        consensus_threshold_percent: int,
        min_swap_amount: int,
        max_swap_amount: int,
        reservation_ttl_secs: int,
    ) -> str:
        admin = self.keypair.pubkey()
        args = layouts.IX_INITIALIZE_ARGS.build(
            {
                'min_collateral': min_collateral,
                'max_collateral': max_collateral,
                'fulfillment_timeout_secs': fulfillment_timeout_secs,
                'consensus_threshold_percent': consensus_threshold_percent,
                'min_swap_amount': min_swap_amount,
                'max_swap_amount': max_swap_amount,
                'reservation_ttl_secs': reservation_ttl_secs,
            }
        )
        metas = [
            AccountMeta(admin, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, True),
            AccountMeta(pdas.treasury_pda(self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('initialize', args, metas)])

    def add_validator(self, validator, weight: int) -> str:
        """Admin: whitelist a validator (bootstrap/test helper). Signer = this client's keypair (admin)."""
        admin = self.keypair.pubkey()
        args = layouts.IX_ADD_VALIDATOR_ARGS.build({'validator': bytes(_as_pubkey(validator)), 'weight': weight})
        metas = [
            AccountMeta(admin, True, False),
            AccountMeta(pdas.config_pda(self.program_id), False, True),
        ]
        return self._send([self._ix('add_validator', args, metas)])

    # ---------- admin runtime config (Context<AdminConfig>: admin signer + config mut) ----------
    def _admin_config(self, name: str, arg_bytes: bytes) -> str:
        admin = self.keypair.pubkey()
        metas = [
            AccountMeta(admin, True, False),
            AccountMeta(pdas.config_pda(self.program_id), False, True),
        ]
        return self._send([self._ix(name, arg_bytes, metas)])

    def remove_validator(self, validator) -> str:
        return self._admin_config(
            'remove_validator', layouts.IX_PUBKEY_ARGS.build({'value': bytes(_as_pubkey(validator))})
        )

    def set_consensus_threshold(self, percent: int) -> str:
        return self._admin_config('set_consensus_threshold', layouts.IX_U8_ARGS.build({'value': percent}))

    def set_fulfillment_timeout(self, secs: int) -> str:
        return self._admin_config('set_fulfillment_timeout', layouts.IX_I64_ARGS.build({'value': secs}))

    def set_halted(self, halted: bool) -> str:
        return self._admin_config('set_halted', layouts.IX_BOOL_ARGS.build({'value': halted}))

    def set_min_collateral(self, amount: int) -> str:
        return self._admin_config('set_min_collateral', layouts.IX_AMOUNT_ARGS.build({'amount': amount}))

    def set_max_collateral(self, amount: int) -> str:
        return self._admin_config('set_max_collateral', layouts.IX_AMOUNT_ARGS.build({'amount': amount}))

    def set_min_swap_amount(self, amount: int) -> str:
        return self._admin_config('set_min_swap_amount', layouts.IX_AMOUNT_ARGS.build({'amount': amount}))

    def set_max_swap_amount(self, amount: int) -> str:
        return self._admin_config('set_max_swap_amount', layouts.IX_AMOUNT_ARGS.build({'amount': amount}))

    def set_reservation_ttl(self, secs: int) -> str:
        return self._admin_config('set_reservation_ttl', layouts.IX_I64_ARGS.build({'value': secs}))

    def set_reservation_fee(self, lamports: int) -> str:
        return self._admin_config('set_reservation_fee', layouts.IX_AMOUNT_ARGS.build({'amount': lamports}))

    def set_pool_window(self, secs: int) -> str:
        return self._admin_config('set_pool_window', layouts.IX_I64_ARGS.build({'value': secs}))

    def set_finalize_window(self, secs: int) -> str:
        return self._admin_config('set_finalize_window', layouts.IX_I64_ARGS.build({'value': secs}))

    def set_weights_update_min_interval(self, secs: int) -> str:
        return self._admin_config('set_weights_update_min_interval', layouts.IX_I64_ARGS.build({'value': secs}))

    def set_max_total_extension(self, secs: int) -> str:
        return self._admin_config('set_max_total_extension', layouts.IX_I64_ARGS.build({'value': secs}))

    def withdraw_treasury(self, recipient, amount: int) -> str:
        """Admin: move accrued protocol fees from the Treasury PDA to `recipient`."""
        admin = self.keypair.pubkey()
        metas = [
            AccountMeta(admin, True, False),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(pdas.treasury_pda(self.program_id), False, True),
            AccountMeta(_as_pubkey(recipient), False, True),
        ]
        return self._send([self._ix('withdraw_treasury', layouts.IX_AMOUNT_ARGS.build({'amount': amount}), metas)])

    # ---------- representative writes (miner-side, no consensus) ----------
    def bind_hotkey(self, hotkey: bytes, hotkey_sig: bytes) -> str:
        """Identity-gated (H3 squat gate): registered miners (MinerState with collateral >=
        min_collateral, so `post_collateral` must have run) and whitelisted validators may bind.
        miner_state is optional on-chain — the program id is passed in its place (anchor's None)
        when the signer has no MinerState."""
        miner = self.keypair.pubkey()
        miner_state = pdas.miner_state_pda(miner, self.program_id)
        if self.get_miner_state(miner) is None:
            miner_state = self.program_id
        metas = [
            AccountMeta(miner, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(miner_state, False, False),
            AccountMeta(pdas.binding_pda(miner, self.program_id), False, True),
            AccountMeta(pdas.hotkey_binding_pda(hotkey, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('bind_hotkey', bytes(hotkey) + bytes(hotkey_sig), metas)])

    def set_quote(
        self, from_chain: str, to_chain: str, miner_from_addr: str, miner_to_addr: str, rate: int, liquidity: int
    ) -> str:
        miner = self.keypair.pubkey()
        args = layouts.IX_SET_QUOTE_ARGS.build(
            {
                'from_chain': from_chain,
                'to_chain': to_chain,
                'miner_from_addr': miner_from_addr,
                'miner_to_addr': miner_to_addr,
                'rate': rate,
                'liquidity': liquidity,
            }
        )
        metas = [
            AccountMeta(miner, True, True),
            AccountMeta(pdas.quote_pda(miner, from_chain, to_chain, self.program_id), False, True),
            AccountMeta(pdas.treasury_pda(self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('set_quote', args, metas)])

    def post_collateral(self, amount: int) -> str:
        miner = self.keypair.pubkey()
        metas = [
            AccountMeta(miner, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(pdas.miner_state_pda(miner, self.program_id), False, True),
            AccountMeta(pdas.collateral_vault_pda(miner, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('post_collateral', layouts.IX_AMOUNT_ARGS.build({'amount': amount}), metas)])

    def withdraw_collateral(self, amount: int) -> str:
        miner = self.keypair.pubkey()
        metas = [
            AccountMeta(miner, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(pdas.miner_state_pda(miner, self.program_id), False, True),
            AccountMeta(pdas.collateral_vault_pda(miner, self.program_id), False, True),
        ]
        return self._send([self._ix('withdraw_collateral', layouts.IX_AMOUNT_ARGS.build({'amount': amount}), metas)])

    def remove_quote(self, from_chain: str, to_chain: str) -> str:
        """Miner retracts one quote-direction; the PDA closes (rent → miner) minus a churn fee → treasury."""
        miner = self.keypair.pubkey()
        args = layouts.IX_REMOVE_QUOTE_ARGS.build({'from_chain': from_chain, 'to_chain': to_chain})
        metas = [
            AccountMeta(miner, True, True),
            AccountMeta(pdas.quote_pda(miner, from_chain, to_chain, self.program_id), False, True),
            AccountMeta(pdas.treasury_pda(self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('remove_quote', args, metas)])

    def deactivate(self) -> str:
        """Miner self-deactivation (no consensus). Guarded on-chain: must be active, no in-flight swap,
        past `busy_until`."""
        miner = self.keypair.pubkey()
        metas = [
            AccountMeta(miner, True, False),
            AccountMeta(pdas.miner_state_pda(miner, self.program_id), False, True),
        ]
        return self._send([self._ix('deactivate', b'', metas)])

    # ---------- swap lifecycle (B2: validator votes + the claim relay) ----------
    def submit_swap_claim(self, miner, swap_key: bytes, from_tx_hash: str, from_tx_block: int) -> str:
        """Validator-relayed: record the winner's source-tx on-chain, creating the Swap in
        PendingAttestation (all terms pinned from the Reservation). swap_key must == keccak(from_tx_hash)."""
        caller = self.keypair.pubkey()
        m = _as_pubkey(miner)
        args = layouts.IX_SUBMIT_CLAIM_ARGS.build(
            {'swap_key': swap_key, 'from_tx_hash': from_tx_hash, 'from_tx_block': from_tx_block}
        )
        metas = [
            AccountMeta(caller, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.reservation_pda(m, self.program_id), False, True),
            AccountMeta(pdas.swap_pda(swap_key, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('submit_swap_claim', args, metas)])

    def vote_initiate(self, swap_key: bytes, miner) -> str:
        """Vote to attest a PendingAttestation swap; on quorum it transitions to Active."""
        validator = self.keypair.pubkey()
        m = _as_pubkey(miner)
        metas = [
            AccountMeta(validator, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),
            AccountMeta(pdas.reservation_pda(m, self.program_id), False, True),
            AccountMeta(pdas.vote_round_pda(pdas.REQ_INITIATE, m, self.program_id), False, True),
            AccountMeta(pdas.swap_pda(swap_key, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        args = layouts.IX_SWAP_KEY_ARGS.build({'swap_key': swap_key})
        return self._send([self._ix('vote_initiate', args, metas)])

    def confirm_swap(self, swap_key: bytes, miner, from_chain: str, to_chain: str) -> str:
        """Vote to confirm a Fulfilled swap; on quorum the fee skims from collateral and the swap closes."""
        validator = self.keypair.pubkey()
        m = _as_pubkey(miner)
        metas = [
            AccountMeta(validator, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),
            AccountMeta(pdas.collateral_vault_pda(m, self.program_id), False, True),
            AccountMeta(pdas.treasury_pda(self.program_id), False, True),
            AccountMeta(pdas.swap_pda(swap_key, self.program_id), False, True),
            AccountMeta(pdas.stats_pda(m, from_chain, to_chain, self.program_id), False, True),
            AccountMeta(pdas.vote_round_pda(pdas.REQ_CONFIRM, swap_key, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        args = layouts.IX_CONFIRM_SWAP_ARGS.build(
            {'swap_key': swap_key, 'from_chain': from_chain, 'to_chain': to_chain}
        )
        return self._send([self._ix('confirm_swap', args, metas)])

    def timeout_swap(self, swap_key: bytes, miner, user) -> str:
        """Vote to time out a swap past its deadline; on quorum collateral is slashed and the user refunded."""
        validator = self.keypair.pubkey()
        m = _as_pubkey(miner)
        metas = [
            AccountMeta(validator, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),
            AccountMeta(pdas.collateral_vault_pda(m, self.program_id), False, True),
            AccountMeta(_as_pubkey(user), False, True),
            AccountMeta(pdas.swap_pda(swap_key, self.program_id), False, True),
            AccountMeta(pdas.vote_round_pda(pdas.REQ_TIMEOUT, swap_key, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        args = layouts.IX_SWAP_KEY_ARGS.build({'swap_key': swap_key})
        return self._send([self._ix('timeout_swap', args, metas)])

    def close_stale_claim(self, miner, swap_key: bytes) -> str:
        """Permissionless: reap an orphaned PendingAttestation claim whose reservation has expired (or was
        superseded). Closes the Swap PDA (rent -> caller) and frees the reservation's claim slot. No slash —
        the claim never obligated the miner. Caller = this client's keypair."""
        caller = self.keypair.pubkey()
        m = _as_pubkey(miner)
        metas = [
            AccountMeta(caller, True, True),
            AccountMeta(m, False, False),
            AccountMeta(pdas.reservation_pda(m, self.program_id), False, True),
            AccountMeta(pdas.swap_pda(swap_key, self.program_id), False, True),
        ]
        args = layouts.IX_SWAP_KEY_ARGS.build({'swap_key': swap_key})
        return self._send([self._ix('close_stale_claim', args, metas)])

    def vote_activate(self, miner) -> str:
        """Vote to activate a miner; on quorum its MinerState flips active=true."""
        validator = self.keypair.pubkey()
        m = _as_pubkey(miner)
        metas = [
            AccountMeta(validator, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),
            AccountMeta(pdas.vote_round_pda(pdas.REQ_ACTIVATE, m, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('vote_activate', b'', metas)])

    def vote_set_weights(self, weights: List[int], validator_keys: List[bytes]) -> str:
        """Vote the validator draw-weight vector (index-aligned to Config.validators); on quorum it applies.
        The round PDA is keyed by the snapshot hash, so competing proposals coexist instead of blocking."""
        round_key = weights_round_key(validator_keys, weights)
        validator = self.keypair.pubkey()
        metas = [
            AccountMeta(validator, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, True),
            AccountMeta(pdas.vote_round_pda(pdas.REQ_SET_WEIGHTS, round_key, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        args = layouts.IX_SET_WEIGHTS_ARGS.build({'weights': weights, 'round_key': round_key})
        return self._send([self._ix('vote_set_weights', args, metas)])

    def mark_fulfilled(self, swap_key: bytes, to_tx_hash: str, to_tx_block: int) -> str:
        """Miner-only: mark an Active swap Fulfilled with the dest-leg tx. Prod miner wiring lands in B4;
        kept here so the test harness can drive a swap to Fulfilled. Signer = this client's keypair."""
        miner = self.keypair.pubkey()
        args = layouts.IX_MARK_FULFILLED_ARGS.build(
            {'swap_key': swap_key, 'to_tx_hash': to_tx_hash, 'to_tx_block': to_tx_block}
        )
        metas = [
            AccountMeta(miner, True, False),
            AccountMeta(pdas.miner_state_pda(miner, self.program_id), False, True),
            AccountMeta(pdas.swap_pda(swap_key, self.program_id), False, True),
        ]
        return self._send([self._ix('mark_fulfilled', args, metas)])

    def extend_timeout(self, swap_key: bytes, miner, target_at: int) -> str:
        """Single-validator slide of a swap's timeout_at forward (no consensus). Loop wiring is a later pass."""
        validator = self.keypair.pubkey()
        m = _as_pubkey(miner)
        metas = [
            AccountMeta(validator, True, False),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),
            AccountMeta(pdas.swap_pda(swap_key, self.program_id), False, True),
        ]
        args = layouts.IX_EXTEND_TIMEOUT_ARGS.build({'swap_key': swap_key, 'target_at': target_at})
        return self._send([self._ix('extend_timeout', args, metas)])

    def extend_reservation(self, miner, target_at: int) -> str:
        """Single-validator slide of a reservation's reserved_until forward (no consensus)."""
        validator = self.keypair.pubkey()
        m = _as_pubkey(miner)
        metas = [
            AccountMeta(validator, True, False),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),
            AccountMeta(pdas.reservation_pda(m, self.program_id), False, True),
        ]
        args = layouts.IX_EXTEND_RESERVATION_ARGS.build({'target_at': target_at})
        return self._send([self._ix('extend_reservation', args, metas)])

    # ---------- swap intake (Phase 9: reservation-lottery pool) ----------
    def open_or_request(self, miner, from_chain: str, to_chain: str) -> str:
        """BID into (or open) a miner's reservation pool for a pair. Signer/payer = this client's keypair
        (the router); pays the flat reservation fee. A bid carries NO taker and NO amounts — the seat
        winner names those in `finalize_reservation`."""
        router = self.keypair.pubkey()
        m = _as_pubkey(miner)
        args = layouts.IX_OPEN_OR_REQUEST_ARGS.build({'from_chain': from_chain, 'to_chain': to_chain})
        metas = [
            AccountMeta(router, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),
            AccountMeta(pdas.quote_pda(m, from_chain, to_chain, self.program_id), False, False),
            AccountMeta(pdas.pool_pda(m, self.program_id), False, True),
            AccountMeta(pdas.treasury_pda(self.program_id), False, True),
            AccountMeta(pdas.reservation_pda(m, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('open_or_request', args, metas)])

    def finalize_reservation(
        self,
        miner,
        user,
        user_from_addr: str,
        user_to_addr: str,
        collateral_amount: int,
        from_amount: int,
        to_amount: int,
    ) -> str:
        """Fill the reservation this client's keypair won at the draw (signer must == reservation.router).
        Names the taker + amounts, running the swap-size bounds + collateral gate + the collateral bind."""
        router = self.keypair.pubkey()
        m = _as_pubkey(miner)
        args = layouts.IX_FINALIZE_RESERVATION_ARGS.build(
            {
                'user': bytes(_as_pubkey(user)),
                'user_from_addr': user_from_addr,
                'user_to_addr': user_to_addr,
                'collateral_amount': collateral_amount,
                'from_amount': from_amount,
                'to_amount': to_amount,
            }
        )
        metas = [
            AccountMeta(router, True, False),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),  # mut: finalize writes busy_until
            AccountMeta(pdas.reservation_pda(m, self.program_id), False, True),
        ]
        return self._send([self._ix('finalize_reservation', args, metas)])

    def close_unfilled_reservation(self, miner) -> str:
        """Permissionless: reap an unfilled reservation past its finalize deadline, freeing the miner."""
        caller = self.keypair.pubkey()
        m = _as_pubkey(miner)
        metas = [
            AccountMeta(caller, True, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),
            AccountMeta(pdas.reservation_pda(m, self.program_id), False, True),
        ]
        return self._send([self._ix('close_unfilled_reservation', b'', metas)])

    def resolve_pool(self, miner) -> str:
        """Permissionless crank: after the pool window closes, run the stake-weighted draw and write the
        winner's Reservation. Signer/payer = this client's keypair (funds the reservation rent)."""
        caller = self.keypair.pubkey()
        m = _as_pubkey(miner)
        metas = [
            AccountMeta(caller, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(m, False, False),
            AccountMeta(pdas.miner_state_pda(m, self.program_id), False, True),
            AccountMeta(pdas.pool_pda(m, self.program_id), False, True),
            AccountMeta(pdas.reservation_pda(m, self.program_id), False, True),
            AccountMeta(SLOT_HASHES, False, False),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('resolve_pool', b'', metas)])

    # ---------- event-log ingest (decode-by-discriminator) ----------
    def get_program_signatures(
        self, before: Optional[str] = None, until: Optional[str] = None, limit: int = 100
    ) -> List[dict]:
        return self.rpc.get_signatures_for_address(self.program_id, before=before, until=until, limit=limit)

    def get_event_logs(self, signature: str) -> List[bytes]:
        """Base64-decoded `Program data:` payloads from a tx's logs (Anchor self-CPI events). The canonical
        event ingest (allways/solana/events.py, used by the validator crown) decodes these."""
        tx = self.rpc.get_transaction(signature)
        if not tx:
            return []
        logs = (tx.get('meta') or {}).get('logMessages') or []
        return [base64.b64decode(line[len('Program data: ') :]) for line in logs if line.startswith('Program data: ')]

    # ---------- dev helper ----------
    def airdrop(self, pubkey, lamports: int) -> str:
        sig = self.rpc.request_airdrop(pubkey, lamports)
        self.rpc.confirm(sig)
        return sig

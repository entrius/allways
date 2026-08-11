"""BondVaultClient — every call into the ``allways_bond_vault`` ink! contract.

Two callers share it: the ``alw vault`` CLI (miner bond ops, permissionless recycle, owner admin)
and the validator's W3 relayer (the quorum rounds that carry Solana verdicts onto the vault —
``vote_slash`` / ``vote_unlock`` / ``vote_collect_fees_batch``).

Reads go through a `ContractsApi` dry-run and return ``None`` when the node can't decode one
(cargo-contract 5.x vs some runtimes) — a read that can't be trusted must never be guessed at, so
every caller treats ``None`` as "unknown", not "zero".

Configuration: ``ALLWAYS_VAULT_ADDRESS`` / the ``vault-address`` config key; metadata JSON via
``ALLWAYS_VAULT_METADATA`` / ``vault-metadata`` (defaulting to the in-repo build artifact);
``ALLWAYS_VAULT_SURI`` overrides the signer for scripting.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from allways.vault import codec

# Repo-relative default: the cargo-contract build artifact.
DEFAULT_METADATA = (
    Path(__file__).resolve().parents[2]
    / 'smart-contracts'
    / 'ink-bond-vault'
    / 'target'
    / 'ink'
    / 'allways_bond_vault.json'
)

# Dry-runs use a generous fixed budget; the actual charge is by weight used.
DEFAULT_GAS = {'ref_time': 300_000_000_000, 'proof_size': 2_000_000}


class VaultConfigError(Exception):
    """The vault address/metadata/signer isn't configured — a user-facing setup problem."""


@dataclass
class VaultCallResult:
    """Outcome of one ``Contracts.call``. A contract-level ``Err`` surfaces as the
    ``ContractReverted`` dispatch error — pallet-contracts does not carry the ink! error
    variant out, so callers that need to tell *which* rejection re-read state instead."""

    ok: bool
    events: List[str] = field(default_factory=list)
    extrinsic_hash: str = '?'
    error: Optional[str] = None

    @property
    def reverted(self) -> bool:
        return self.error == 'ContractReverted'


@dataclass(frozen=True)
class VaultEvent:
    block: int
    name: str
    fields: Dict[str, Any]


def resolve_vault_address(config=None) -> str:
    addr = os.environ.get('ALLWAYS_VAULT_ADDRESS') or (config.get('vault-address') if config else None)
    if not addr:
        raise VaultConfigError(
            'Vault address not configured — `alw config set vault-address <ss58>` or set ALLWAYS_VAULT_ADDRESS'
        )
    return addr


def resolve_metadata_path(config=None) -> str:
    return (
        os.environ.get('ALLWAYS_VAULT_METADATA')
        or (config.get('vault-metadata') if config else None)
        or str(DEFAULT_METADATA)
    )


def resolve_signer(wallet, use_coldkey: bool = False):
    """The keypair that signs vault calls: the wallet HOTKEY by default (the vault keys bonds by
    hotkey, and validators are registered in its set by hotkey), overridable via ALLWAYS_VAULT_SURI."""
    suri = os.environ.get('ALLWAYS_VAULT_SURI')
    if suri:
        import bittensor as bt

        return bt.Keypair.create_from_uri(suri)
    return wallet.coldkey if use_coldkey else wallet.hotkey


class BondVaultClient:
    def __init__(
        self,
        subtensor,
        address: str,
        keypair=None,
        metadata: Optional[codec.VaultMetadata] = None,
        metadata_path: Optional[str] = None,
        gas: Optional[dict] = None,
    ):
        self.subtensor = subtensor
        self.address = address
        self.keypair = keypair
        self.metadata = metadata or codec.VaultMetadata.from_path(metadata_path or str(DEFAULT_METADATA))
        self.gas = gas or DEFAULT_GAS

    @classmethod
    def from_config(cls, subtensor, config=None, keypair=None, **kwargs) -> 'BondVaultClient':
        """Build from the env/config keys. Raises ``VaultConfigError`` when the vault isn't set up."""
        return cls(
            subtensor,
            resolve_vault_address(config),
            keypair=keypair,
            metadata_path=resolve_metadata_path(config),
            **kwargs,
        )

    # ─── transport ───────────────────────────────────────────────────────────

    def submit(self, data: bytes, value: int = 0, keypair=None) -> VaultCallResult:
        signer = keypair or self.keypair
        if signer is None:
            raise VaultConfigError('No signer configured for vault writes')
        call = self.subtensor.substrate.compose_call(
            call_module='Contracts',
            call_function='call',
            call_params={
                'dest': self.address,
                'value': value,
                'gas_limit': self.gas,
                'storage_deposit_limit': None,
                'data': '0x' + data.hex(),
            },
        )
        ext = self.subtensor.substrate.create_signed_extrinsic(call=call, keypair=signer)
        receipt = self.subtensor.substrate.submit_extrinsic(ext, wait_for_inclusion=True)
        return self._classify(receipt)

    @staticmethod
    def _classify(receipt) -> VaultCallResult:
        names = _event_names(receipt)
        failed = any('ExtrinsicFailed' in n for n in names) or not getattr(receipt, 'is_success', True)
        error = None
        if failed:
            try:
                em = receipt.error_message
                error = em.get('name') if isinstance(em, dict) else None
            except Exception:
                error = None
        return VaultCallResult(
            ok=not failed,
            events=names,
            extrinsic_hash=str(getattr(receipt, 'extrinsic_hash', '?')),
            error=error,
        )

    def dry_read(self, data: bytes, origin: Optional[str] = None) -> Optional[bytes]:
        """Best-effort contract read. Returns the unwrapped return payload, or None when the node's
        `ContractResult` isn't decodable client-side (a known cargo-contract/runtime skew)."""
        who = origin or (self.keypair.ss58_address if self.keypair is not None else self.address)
        try:
            res = self.subtensor.substrate.runtime_call(
                'ContractsApi',
                'call',
                {
                    'origin': who,
                    'dest': self.address,
                    'value': 0,
                    'gas_limit': None,
                    'storage_deposit_limit': None,
                    'input_data': '0x' + data.hex(),
                },
            )
            v = res.value if hasattr(res, 'value') else res
            return codec.unwrap_lang_error(_return_bytes(v['result']['Ok']['data']))
        except Exception:
            return None

    def _read(self, label: str, *args: bytes) -> Optional[codec.Reader]:
        raw = self.dry_read(self.metadata.call(label, *args))
        return codec.Reader(raw) if raw else None

    def _read_u64(self, label: str, *args: bytes) -> Optional[int]:
        reader = self._read(label, *args)
        try:
            return reader.uint(8) if reader is not None else None
        except codec.VaultCodecError:
            return None

    # ─── reads ───────────────────────────────────────────────────────────────

    def get_collateral(self, hotkey: str) -> Optional[int]:
        """The miner's GROSS bond on the vault's books (rao) — not net of unsettled fees."""
        return self._read_u64('get_collateral', codec.account_bytes(hotkey))

    def get_lock_state(self, hotkey: str) -> Optional[Tuple[bool, int]]:
        """``(locked, lock_epoch)``. The epoch is what ``vote_unlock`` must name."""
        reader = self._read('get_lock_state', codec.account_bytes(hotkey))
        if reader is None:
            return None
        try:
            return reader.boolean(), reader.uint(8)
        except codec.VaultCodecError:
            return None

    def get_settled_total(self, hotkey: str) -> Optional[int]:
        """Cumulative protocol fees already settled onto the vault for this miner (rao)."""
        return self._read_u64('get_settled_total', codec.account_bytes(hotkey))

    def is_slashed(self, swap_ref) -> Optional[bool]:
        """Whether the vault has already applied a slash for this Solana swap key — the permanent
        marker that makes ``vote_slash`` replay-proof."""
        reader = self._read('is_slashed', codec.hash32(swap_ref))
        if reader is None:
            return None
        try:
            return reader.boolean()
        except codec.VaultCodecError:
            return None

    def get_pending_slash(self, swap_ref) -> Optional[Tuple[str, int]]:
        """A reimbursement parked for ``claim_slash`` (the push transfer failed), or None."""
        reader = self._read('get_pending_slash', codec.hash32(swap_ref))
        if reader is None:
            return None
        try:
            if not reader.boolean():  # Option::None
                return None
            return reader.account(), reader.uint(8)
        except codec.VaultCodecError:
            return None

    def get_accumulated_fees(self) -> Optional[int]:
        return self._read_u64('get_accumulated_fees')

    def get_total_recycled_fees(self) -> Optional[int]:
        return self._read_u64('get_total_recycled_fees')

    def get_recyclable_pot(self) -> Optional[int]:
        """What the next ``recycle_fees`` would actually drain (rao): settled fees PLUS any TAO
        transferred straight to the vault address. Exceeds ``get_accumulated_fees`` by the donated
        share — the pot is derived from the real balance, not from the fee counter."""
        return self._read_u64('get_recyclable_pot')

    def get_total_collateral(self) -> Optional[int]:
        """Total bonds owed to miners — the first term of the commingling invariant, and the
        floor the recyclable pot can never dip below."""
        return self._read_u64('get_total_collateral')

    def get_pending_slash_total(self) -> Optional[int]:
        """Unclaimed slash reimbursements owed to users."""
        return self._read_u64('get_pending_slash_total')

    def get_min_collateral(self) -> Optional[int]:
        return self._read_u64('get_min_collateral')

    def get_max_collateral(self) -> Optional[int]:
        return self._read_u64('get_max_collateral')

    def get_consensus_threshold(self) -> Optional[int]:
        reader = self._read('get_consensus_threshold')
        try:
            return reader.uint(1) if reader is not None else None
        except codec.VaultCodecError:
            return None

    def get_vote_round_ttl(self) -> Optional[int]:
        reader = self._read('get_vote_round_ttl')
        try:
            return reader.uint(4) if reader is not None else None
        except codec.VaultCodecError:
            return None

    def get_validators(self) -> Optional[List[str]]:
        """The current validator set. Its SIZE is the governance bar: add/config need every
        member, removal every member but the target."""
        return self._read_accounts('get_validators')

    def get_pending_validators(self) -> Optional[List[str]]:
        """Candidates a unanimous round approved, still awaiting their own ``accept_validator``.
        They count toward no quorum until they accept."""
        return self._read_accounts('get_pending_validators')

    def _read_accounts(self, label: str) -> Optional[List[str]]:
        reader = self._read(label)
        if reader is None:
            return None
        try:
            return [reader.account() for _ in range(reader.compact())]
        except codec.VaultCodecError:
            return None

    # ─── validator quorum rounds (the W3 relays) ─────────────────────────────

    def vote_slash(
        self, hotkey: str, swap_ref, penalty: int, user: str, reimbursement: int, keypair=None
    ) -> VaultCallResult:
        """Carry a Solana timeout verdict onto the vault. Every argument is hash-bound into the
        round, so all validators must relay the event's ABSOLUTE figures verbatim — a reconstructed
        penalty conflicts instead of co-counting."""
        return self.submit(
            self.metadata.call(
                'vote_slash',
                codec.account_bytes(hotkey),
                codec.hash32(swap_ref),
                codec.u64(penalty),
                codec.account_bytes(user),
                codec.u64(reimbursement),
            ),
            keypair=keypair,
        )

    def vote_unlock(self, hotkey: str, epoch: int, keypair=None) -> VaultCallResult:
        """Release a bond after the exit sequence. The epoch is hash-bound, so a stale round can
        never unlock a bond that has since re-locked."""
        return self.submit(
            self.metadata.call('vote_unlock', codec.account_bytes(hotkey), codec.u64(epoch)),
            keypair=keypair,
        )

    def vote_collect_fees_batch(self, entries: Sequence[Tuple[str, int]], keypair=None) -> VaultCallResult:
        """Settle cumulative fee totals. The round key is the batch-contents hash, so the entry
        ORDER is consensus-relevant — the caller owns the deterministic ordering."""
        return self.submit(
            self.metadata.call('vote_collect_fees_batch', codec.fee_entries(entries)),
            keypair=keypair,
        )

    # ─── miner + permissionless surface ──────────────────────────────────────

    def post_collateral(self, rao: int, keypair=None) -> VaultCallResult:
        return self.submit(self.metadata.call('post_collateral'), value=rao, keypair=keypair)

    def lock_bond(self, keypair=None) -> VaultCallResult:
        return self.submit(self.metadata.call('lock_bond'), keypair=keypair)

    def withdraw_collateral(self, rao: int, keypair=None) -> VaultCallResult:
        return self.submit(self.metadata.call('withdraw_collateral', codec.u64(rao)), keypair=keypair)

    def claim_slash(self, swap_ref, keypair=None) -> VaultCallResult:
        return self.submit(self.metadata.call('claim_slash', codec.hash32(swap_ref)), keypair=keypair)

    def recycle_fees(self, keypair=None) -> VaultCallResult:
        return self.submit(self.metadata.call('recycle_fees'), keypair=keypair)

    def admin_call(self, label: str, *args: bytes, keypair=None) -> VaultCallResult:
        """Owner-only config/validator-set setters. No admin path touches funds."""
        return self.submit(self.metadata.call(label, *args), keypair=keypair)

    # ─── events ──────────────────────────────────────────────────────────────

    def head(self) -> Optional[int]:
        """Current chain head, for the event-poll cursor. Read through the same substrate that
        serves the events, so the cursor and the events can never be from different worlds."""
        try:
            return int(self.subtensor.substrate.get_block_number(self.subtensor.substrate.get_chain_head()))
        except Exception as e:
            bt_debug(f'vault head read failed: {e}')
            return None

    def poll_events(self, start_block: int, end_block: int) -> List[VaultEvent]:
        """Vault events emitted in ``[start_block, end_block]``, oldest first.

        Best-effort by design: a block whose events can't be read or decoded is skipped rather than
        raising, because the relayer's reconcile loop already re-derives the same facts from state.
        Events are the fast path, never the source of truth."""
        out: List[VaultEvent] = []
        for block in range(max(0, start_block), end_block + 1):
            for topics, data in self._contract_emissions(block):
                spec = self.metadata.spec_for_topic(topics[0]) if topics else None
                if spec is None:
                    continue
                try:
                    fields = self.metadata.decode_event(spec, data)
                except codec.VaultCodecError:
                    continue
                out.append(VaultEvent(block, spec.label, fields))
        return out

    def _contract_emissions(self, block: int) -> List[Tuple[List[str], bytes]]:
        """`Contracts.ContractEmitted` payloads THIS vault emitted in one block."""
        try:
            block_hash = self.subtensor.substrate.get_block_hash(block)
            events = self.subtensor.substrate.get_events(block_hash)
        except Exception:
            return []
        out: List[Tuple[List[str], bytes]] = []
        for ev in events or []:
            record = ev.value if hasattr(ev, 'value') else ev
            body = record.get('event', record) if isinstance(record, dict) else None
            if not isinstance(body, dict):
                continue
            if (body.get('module_id') or body.get('module')) != 'Contracts':
                continue
            if (body.get('event_id') or body.get('name')) != 'ContractEmitted':
                continue
            attrs = body.get('attributes') or {}
            if not isinstance(attrs, dict) or str(attrs.get('contract')) != self.address:
                continue
            raw = _return_bytes(attrs.get('data') or b'')
            # Topics sit on the event RECORD, beside the event rather than inside its attributes.
            topics = [str(t) for t in (_record_topics(record) or attrs.get('topics') or [])]
            out.append((topics, raw))
        return out


def _record_topics(record) -> List[str]:
    return list(record.get('topics') or []) if isinstance(record, dict) else []


def _return_bytes(payload) -> bytes:
    """The dry-run's return blob, whatever shape the node's codec handed it back in.

    A SCALE `Bytes` decodes to a hex STRING when the payload isn't valid UTF-8 and to the
    utf-8-decoded characters when it is — so the same query answers in two shapes depending on
    its own value, and small/zero numbers land in the second one."""
    if isinstance(payload, str):
        return bytes.fromhex(payload[2:]) if payload.startswith('0x') else payload.encode('utf-8')
    return bytes(payload)


def bt_debug(message: str) -> None:
    """Debug logging that never drags bittensor into a CLI import path that doesn't need it."""
    try:
        import bittensor as bt

        bt.logging.debug(message)
    except Exception:
        pass


def _event_names(receipt) -> List[str]:
    names: List[str] = []
    try:
        for ev in receipt.triggered_events:
            v = ev.value if hasattr(ev, 'value') else ev
            e = v.get('event', v) if isinstance(v, dict) else v
            mod = e.get('module_id') or e.get('module') or '?'
            eid = e.get('event_id') or e.get('name') or '?'
            names.append(f'{mod}.{eid}')
    except Exception:
        pass
    return names

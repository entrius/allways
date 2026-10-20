"""Which accounts each program event touched — the World's whole refresh policy, in one table.

An event either carries the account's new state outright (quotes) or names accounts the World re-reads
in its next batched read. Over-reading is cheap and under-reading is the bug, so any event naming a
miner re-reads that miner's state, reservation and pool on the named hub (every hub when none is named)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Set, Tuple

from construct import Container

from allways.solana import pdas
from allways.solana.events import TX_LOGS_TRUNCATED
from allways.validator.hubs import HUBS

AccountKey = Tuple[str, str]  # (account type, pda as str)
UNTYPED = ''  # an account of unknown type: the read names it from its owner and discriminator

CONFIG_EVENTS = frozenset({'ConfigChanged', 'HaltSet', 'ValidatorWeightsUpdated', 'AttestHeartbeat'})
NO_ACCOUNT_EVENTS = frozenset({'TreasuryWithdrawn'})


@dataclass
class Touches:
    reads: Set[AccountKey] = field(default_factory=set)
    quote_upserts: List[Tuple[str, Any]] = field(default_factory=list)  # (pda, MinerQuote-shaped account)
    quote_removals: List[str] = field(default_factory=list)  # pda


def _hubs(fields: Any) -> List[str]:
    named = fields.get('collateral_chain') if hasattr(fields, 'get') else None
    return [str(named)] if named else [h.chain for h in HUBS]


def _miner_reads(miner: Any, hubs: List[str], program_id: Any) -> Set[AccountKey]:
    keys = {('MinerState', str(pdas.miner_state_pda(miner, program_id)))}
    for hub in hubs:
        keys.add(('Reservation', str(pdas.reservation_pda(miner, hub, program_id))))
        keys.add(('Pool', str(pdas.pool_pda(miner, hub, program_id))))
    return keys


def quote_from_event(fields: Any) -> Container:
    """The MinerQuote account a QuoteSet wrote: the event carries every field but the PDA bump."""
    keys = ('from_chain', 'to_chain', 'collateral_chain', 'miner_from_addr', 'miner_to_addr')
    return Container(
        miner=bytes(fields['miner']),
        **{k: str(fields[k]) for k in keys},
        rate=int(fields['rate']),
        liquidity=int(fields['liquidity']),
        updated_at=int(fields['updated_at']),
        bump=0,
    )


def touches(name: str, fields: Any, program_id: Optional[Any] = None) -> Touches:
    out = Touches()
    if name in NO_ACCOUNT_EVENTS:
        return out
    if name == TX_LOGS_TRUNCATED:
        out.reads |= {(UNTYPED, str(pk)) for pk in fields.get('accounts') or ()}  # typed at read time
        return out
    if name in CONFIG_EVENTS:
        out.reads.add(('Config', str(pdas.config_pda(program_id))))
        return out
    miner = fields.get('miner')
    if name in ('QuoteSet', 'QuoteRemoved'):
        pda = str(
            pdas.quote_pda(
                miner, str(fields['from_chain']), str(fields['to_chain']), str(fields['collateral_chain']), program_id
            )
        )
        if name == 'QuoteSet' and 'miner_from_addr' in fields:
            out.quote_upserts.append((pda, quote_from_event(fields)))
        elif name == 'QuoteSet':
            out.reads.add(('MinerQuote', pda))  # a pre-upgrade event without the addresses
        else:
            out.quote_removals.append(pda)
        return out
    if miner is not None:
        out.reads |= _miner_reads(miner, _hubs(fields), program_id)
    if fields.get('swap_key') is not None:
        out.reads.add(('Swap', str(pdas.swap_pda(bytes(fields['swap_key']), program_id))))
    if name == 'HotkeyBound':
        out.reads.add(('Binding', str(pdas.binding_pda(miner, program_id))))
    if name == 'BondAttested':
        out.reads.add(('BondAttestation', str(pdas.bond_attestation_pda(miner, str(fields['chain']), program_id))))
    return out

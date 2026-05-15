"""Translate raw validator rejection_reason strings into actionable CLI messages.

Validator-side rejection strings are stable, terse identifiers — fine for logs,
opaque for users. This module aggregates per-validator responses, prints one
line per validator, and (when the rejections agree) renders a single
human-readable explanation plus a deterministic-vs-transient flag the caller
uses to decide whether to prompt for retry.

Mapping is prefix-matched on the lowercased reason so minor wording changes on
the validator side don't silently regress the UX. Any reason that does not
prefix-match falls through to the raw string — so adding a new validator-side
rejection still surfaces something useful, just untranslated.
"""

from dataclasses import dataclass, field
from typing import Callable, Optional

from rich.console import Console


@dataclass
class RejectionInfo:
    """Aggregated outcome of a multi-validator broadcast.

    Attributes:
        accepted: validators that returned accepted=True.
        queued: subset of accepted where the validator queued for confirmations.
        rejected: validators that responded with a rejection_reason.
        no_response: validators that timed out / didn't respond.
        headline: user-facing translated message when all rejections agree;
            empty when validators disagreed or accepted >= 1.
        deterministic: True when retrying with identical inputs cannot succeed.
            Only meaningful when accepted == 0.
        category: matched rule key (e.g. 'insufficient_source_balance') or
            'mixed' / 'unmatched' / 'no_response_only' / ''. Useful for tests.
        raw_reasons: list of raw rejection strings (one per non-accepted
            validator; '' for no-response).
    """

    accepted: int = 0
    queued: int = 0
    rejected: int = 0
    no_response: int = 0
    headline: str = ''
    deterministic: bool = False
    category: str = ''
    raw_reasons: list[str] = field(default_factory=list)


# Rule = (category_key, prefix, deterministic, builder)
# Order: most specific first. Prefix match is case-insensitive.
_Rule = tuple[str, str, bool, Callable[[dict], str]]


def _ctx_get(ctx: dict, key: str, fallback: str = '?') -> str:
    val = ctx.get(key)
    return str(val) if val not in (None, '') else fallback


_RULES: list[_Rule] = [
    # ------ SwapReserve rejections ------
    (
        'insufficient_source_balance',
        'insufficient source balance',
        True,
        lambda ctx: (
            f'Source address [yellow]{_ctx_get(ctx, "from_address", "<your source address>")}[/yellow] '
            f'does not hold enough {_ctx_get(ctx, "from_chain_upper", "funds")}. '
            f'Fund it with at least [bold]{_ctx_get(ctx, "from_amount_human")} '
            f'{_ctx_get(ctx, "from_chain_upper", "")}[/bold] and try again.'
        ),
    ),
    (
        'invalid_source_proof',
        'invalid source address proof',
        True,
        lambda ctx: (
            f'Signature for [yellow]{_ctx_get(ctx, "from_address", "<your source address>")}[/yellow] '
            'did not verify. The signing key (BTC_PRIVATE_KEY/WIF or coldkey) does not match '
            'the source address.'
        ),
    ),
    (
        'address_cooldown',
        'address on cooldown',
        True,
        lambda ctx: (
            f'Source address [yellow]{_ctx_get(ctx, "from_address", "<your source address>")}[/yellow] '
            f'is cooling down — {_ctx_get(ctx, "raw_reason", "").removeprefix("Address on cooldown: ")}. '
            f'Each failed reservation doubles the next cooldown; wait it out or reserve from a different '
            'address.'
        ),
    ),
    (
        'wrong_direction',
        'miner does not support this swap direction',
        True,
        lambda ctx: (
            f'Miner UID {_ctx_get(ctx, "miner_uid")} does not quote a rate for '
            f'{_ctx_get(ctx, "from_chain_upper")} → {_ctx_get(ctx, "to_chain_upper")}. '
            'Pick a different miner.'
        ),
    ),
    (
        'no_valid_commitment',
        'no valid commitment',
        True,
        lambda ctx: (
            f'Miner UID {_ctx_get(ctx, "miner_uid")} has no valid rate commitment on-chain. Pick a different miner.'
        ),
    ),
    (
        'miner_not_active',
        'miner not active',
        True,
        lambda ctx: f'Miner UID {_ctx_get(ctx, "miner_uid")} is not active on the contract. Pick a different miner.',
    ),
    (
        'miner_busy',
        'miner has an active swap',
        False,
        lambda ctx: (
            f'Miner UID {_ctx_get(ctx, "miner_uid")} is currently fulfilling another swap. '
            'Wait a few blocks or pick another miner.'
        ),
    ),
    (
        'miner_already_reserved',
        'miner already reserved',
        False,
        lambda ctx: (
            f'Miner UID {_ctx_get(ctx, "miner_uid")} is already reserved by someone else. '
            'Wait for the reservation to clear or pick another miner.'
        ),
    ),
    (
        'insufficient_miner_collateral',
        'insufficient miner collateral',
        True,
        lambda ctx: (
            f'Miner UID {_ctx_get(ctx, "miner_uid")} does not have enough collateral for this swap '
            'amount. Try a smaller amount or another miner.'
        ),
    ),
    (
        'miner_collateral_below_minimum',
        'miner collateral below minimum',
        True,
        lambda ctx: (
            f'Miner UID {_ctx_get(ctx, "miner_uid")} has fallen below the minimum collateral '
            'requirement. Pick a different miner.'
        ),
    ),
    (
        'swap_below_min',
        'swap amount below minimum',
        True,
        lambda ctx: (
            f'Swap amount is below the protocol minimum ({_ctx_get(ctx, "raw_reason", "")}). Try a larger amount.'
        ),
    ),
    (
        'swap_above_max',
        'swap amount above maximum',
        True,
        lambda ctx: (
            f'Swap amount is above the protocol maximum ({_ctx_get(ctx, "raw_reason", "")}). Try a smaller amount.'
        ),
    ),
    (
        'same_chain',
        'source and destination chains must be different',
        True,
        lambda ctx: 'Source and destination chains must differ.',
    ),
    # ------ SwapConfirm rejections ------
    (
        'no_active_reservation',
        'no active reservation for this miner',
        True,
        lambda ctx: (
            f'Miner UID {_ctx_get(ctx, "miner_uid")} no longer has an active reservation — it likely '
            'expired before your tx was confirmed. Start a new swap with: alw swap now'
        ),
    ),
    (
        'reservation_data_missing',
        'reservation data not found',
        True,
        lambda ctx: 'The reservation has already been initiated or cleared. Check status: alw view reservation',
    ),
    (
        'invalid_tx_proof',
        'invalid source tx proof',
        True,
        lambda ctx: (
            'Signature over the source tx hash did not verify. The signing key does not match '
            f'[yellow]{_ctx_get(ctx, "from_address", "<your source address>")}[/yellow].'
        ),
    ),
    (
        'invalid_dest_address',
        'invalid destination address format',
        True,
        lambda ctx: (
            f'Destination address is not a valid {_ctx_get(ctx, "to_chain_upper", "destination chain")} '
            'address. Re-run the swap with the correct receive address.'
        ),
    ),
    (
        'unsupported_dest_chain',
        'unsupported destination chain',
        True,
        lambda ctx: f'Validator does not support this destination chain ({_ctx_get(ctx, "raw_reason", "")}).',
    ),
    (
        'tx_not_found',
        'source transaction not found',
        False,
        # The same validator path that rejects here will queue the tx with
        # `0/N confirmations` once it propagates, so this is almost always
        # just a freshly broadcast tx that hasn't reached validator nodes
        # yet. Frame it as a wait, not a hard error — a stern "could not
        # find" message has scared users into thinking the swap failed.
        lambda ctx: (
            'Source tx not yet visible to validators — usually just propagation lag. '
            'Retry in a few seconds with the resume hint below; validators will queue and '
            'auto-initiate once the tx lands.\n'
            '[dim]If it keeps rejecting, the hash or sender may be off — '
            'pass [cyan]--block <N>[/cyan] if you have it.[/dim]'
        ),
    ),
    (
        'missing_dest_address',
        'missing destination address',
        True,
        lambda ctx: 'Internal: missing destination address in the request.',
    ),
    # ------ MinerActivate rejections ------
    (
        'miner_not_registered',
        'hotkey not registered on subnet',
        True,
        lambda ctx: 'Your hotkey is not registered on this subnet. Register first: btcli subnets register',
    ),
    (
        'miner_no_commitment',
        'no commitment found',
        True,
        lambda ctx: 'No trading-pair commitment found for your miner. Run: alw pair set',
    ),
    (
        'already_active',
        'miner is already active',
        True,
        lambda ctx: 'Your miner is already active.',
    ),
    (
        'insufficient_collateral',
        'insufficient collateral',
        True,
        lambda ctx: f'Insufficient collateral ({_ctx_get(ctx, "raw_reason", "")}). Top up with: alw collateral deposit',
    ),
    # ------ Generic / shared ------
    (
        'unsupported_chain',
        'unsupported chain',
        True,
        lambda ctx: f'Validator does not support this chain ({_ctx_get(ctx, "raw_reason", "")}).',
    ),
    (
        'missing_src_proof',
        'missing source address or proof',
        True,
        lambda ctx: 'Internal: missing source address or proof in the request.',
    ),
    (
        'missing_chain_fields',
        'missing from_chain or to_chain',
        True,
        lambda ctx: 'Internal: missing chain fields in the request.',
    ),
    (
        'duplicate_source_tx',
        'vote_initiate: duplicatesourcetx',
        True,
        lambda ctx: (
            'Source transaction was already used in a prior swap, so the contract rejected '
            'the new initiation. Start a fresh swap with [bold]alw swap[/bold] so a new '
            'source transaction is broadcast.'
        ),
    ),
    (
        'contract_rejected',
        'contract rejected',
        False,
        lambda ctx: 'Contract rejected the request on-chain — usually transient. Retrying may help.',
    ),
]


def _match_rule(reason: str) -> Optional[_Rule]:
    if not reason:
        return None
    needle = reason.lower()
    for rule in _RULES:
        _, prefix, _, _ = rule
        if needle.startswith(prefix):
            return rule
    return None


def render_and_aggregate(
    console: Console,
    responses,
    *,
    label: str = 'V',
    context: Optional[dict] = None,
) -> RejectionInfo:
    """Print per-validator status lines and return aggregate counts + headline.

    Each response is rendered as one line:
      ``{label}{i}: ok``
      ``{label}{i}: queued <reason>``       (accepted but waiting for confirmations)
      ``{label}{i}: no <raw reason>``       (rejected)
      ``{label}{i}: no response — timeout`` (no rejection_reason returned)

    When ``accepted == 0`` and every rejection prefix-matches the same rule,
    a translated headline is set on the returned info. Mixed-reason failures
    fall back to ``headline=''`` so the caller can render a generic line.
    """
    ctx = dict(context or {})
    info = RejectionInfo()

    for i, resp in enumerate(responses, 1):
        accepted = bool(getattr(resp, 'accepted', None))
        raw = (getattr(resp, 'rejection_reason', '') or '').strip()
        if accepted:
            info.accepted += 1
            if raw and 'queued' in raw.lower():
                info.queued += 1
                console.print(f'  {label}{i}: [yellow]queued[/yellow] {raw}')
            else:
                # Blank reason on accept is normal — don't print the no-response fallback.
                console.print(f'  {label}{i}: [green]ok[/green]')
            continue

        info.raw_reasons.append(raw)
        if not raw:
            info.no_response += 1
            console.print(f'  {label}{i}: [yellow]no response[/yellow] [dim]— timeout or validator down[/dim]')
        else:
            info.rejected += 1
            console.print(f'  {label}{i}: [red]no[/red] [dim]{raw}[/dim]')

    if info.accepted > 0 or (info.rejected == 0 and info.no_response == 0):
        return info

    # No-response-only: deterministic=False (transient by nature).
    if info.rejected == 0 and info.no_response > 0:
        info.category = 'no_response_only'
        info.headline = 'No validators responded within the timeout — the chain may be slow or validators may be down.'
        info.deterministic = False
        return info

    # Match every rejected reason to a rule. If they all map to the same
    # rule, render the translated headline; otherwise call it mixed.
    matched_categories: set[str] = set()
    last_match: Optional[_Rule] = None
    last_raw = ''
    unmatched = False
    for raw in info.raw_reasons:
        if not raw:
            continue
        rule = _match_rule(raw)
        if rule is None:
            unmatched = True
            last_raw = raw
            continue
        matched_categories.add(rule[0])
        last_match = rule
        last_raw = raw

    if last_match is None:
        # No rejection prefix-matched any rule — surface the raw reason as the headline
        # so the user still gets *something* actionable. Treat as transient by default
        # so the user can choose to retry.
        info.category = 'unmatched'
        info.headline = last_raw or 'Validators rejected the request.'
        info.deterministic = False
        return info

    if len(matched_categories) > 1 or unmatched:
        info.category = 'mixed'
        info.headline = ''
        # Mixed deterministic flags → call it transient so the user can retry.
        info.deterministic = False
        return info

    category, _, det, builder = last_match
    ctx.setdefault('raw_reason', last_raw)
    info.category = category
    info.deterministic = det
    try:
        info.headline = builder(ctx)
    except Exception:
        # A formatting error in the builder must not crash the CLI — fall back to the raw reason.
        info.headline = last_raw
    return info

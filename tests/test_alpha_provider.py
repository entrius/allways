"""Alpha settlement: a top-level transfer_stake that succeeded AND emitted StakeTransferred, amount from the CALL."""

from types import SimpleNamespace

import pytest

from allways.assets.alpha import Alpha
from allways.assets.asset import ProviderUnreachableError
from allways.assets.tao import BETA_ESCROW, TaoChain
from allways.chains import CHAIN_SN7

MINER = 'minerCold'
USER = 'userCold'
HOTKEY = 'hotkeyA'
TXID_BYTES = bytes.fromhex('ab' * 32)
TXID = '0x' + TXID_BYTES.hex()
BLOCK = 500
HEAD = BLOCK + 10
NETUID = CHAIN_SN7.netuid


def _ext(
    alpha=5_000,
    netuid=NETUID,
    dest=MINER,
    sender=USER,
    module='SubtensorModule',
    function='transfer_stake',
    origin_netuid=None,
):
    """The real substrate.get_block shape: a GenericExtrinsic with bytes .extrinsic_hash and a .value dict."""
    value = {
        'address': sender,
        'call': {
            'call_module': module,
            'call_function': function,
            'call_args': [
                {'name': 'destination_coldkey', 'value': dest},
                {'name': 'hotkey', 'value': HOTKEY},
                {'name': 'origin_netuid', 'value': netuid if origin_netuid is None else origin_netuid},
                {'name': 'destination_netuid', 'value': netuid},
                {'name': 'alpha_amount', 'value': alpha},
            ],
        },
    }
    return SimpleNamespace(extrinsic_hash=TXID_BYTES, value=value)


def _batched(inner):
    value = {
        'address': USER,
        'call': {
            'call_module': 'Utility',
            'call_function': 'batch',
            'call_args': [{'name': 'calls', 'value': [inner.value]}],
        },
    }
    return SimpleNamespace(extrinsic_hash=TXID_BYTES, value=value)


def _event(module, name, attributes=None, idx=0):
    return {'extrinsic_idx': idx, 'event': {'module_id': module, 'event_id': name, 'attributes': attributes or {}}}


def _settled_events(idx=0, tao_amount=123):
    return [
        _event('SubtensorModule', 'StakeTransferred', (USER, MINER, HOTKEY, NETUID, NETUID, tao_amount), idx),
        _event('System', 'ExtrinsicSuccess', idx=idx),
    ]


def _provider(*, exts=None, events=None, block_time=1_700_000_000, wallet=None):
    p = Alpha(CHAIN_SN7, SimpleNamespace(get_current_block=lambda: HEAD), wallet)
    block = {'extrinsics': [_ext()] if exts is None else exts}
    p.chain.get_block = lambda n: block if n == BLOCK else {'extrinsics': []}
    p.chain.get_block_hash = lambda n: f'0xblock{n}'
    p.chain.get_block_events = lambda h: _settled_events() if events is None else events
    p.chain.get_block_time = lambda n: block_time
    return p


def _verify(p, amount=5_000):
    return p.fetch_matching_tx(TXID, MINER, amount, block_hint=BLOCK)


# ─── registry + seam ────────────────────────────────────────────────────────


def test_beta_escrow_is_never_a_valid_payee_on_tao_or_alpha():
    # Keyless protocol custody: transfer_stake into it fails (CannotUseSystemAccount), TAO sent there is
    # stranded. Invalid at the chain, so reserve refuses it and an in-flight swap cancels no-fault.
    assert BETA_ESCROW == '5EYCAe5jLQhn6ofDSwHx3AZmsZPVFHnKpstqap4vqwDWtp7s'
    for chain in (TaoChain(SimpleNamespace()), Alpha(CHAIN_SN7, SimpleNamespace()).chain):
        assert not chain.is_valid_address(BETA_ESCROW)
        assert chain.is_valid_address('5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY')


# ─── verification ───────────────────────────────────────────────────────────


def test_amount_comes_from_the_call_not_the_event():
    """StakeTransferred carries the TAO-equivalent (123); the leg is worth the call's 5000 alpha."""
    info = _verify(_provider(events=_settled_events(tao_amount=123)))
    assert info is not None
    assert (info.sender, info.recipient, info.amount, info.block_number) == (USER, MINER, 5_000, BLOCK)
    assert info.block_time == 1_700_000_000


def test_batched_transfer_stake_is_rejected():
    assert _verify(_provider(exts=[_batched(_ext())])) is None


def test_included_but_failed_transfer_stake_is_not_settled():
    failed = [_event('System', 'ExtrinsicFailed', {'dispatch_error': {'Module': 'TransferDisallowed'}})]
    assert _verify(_provider(events=failed)) is None
    assert _verify(_provider(events=[_event('System', 'ExtrinsicSuccess')])) is None
    assert _verify(_provider(events=[_settled_events()[0]])) is None


def test_cross_netuid_transfer_is_not_credited():
    """alpha_amount is denominated in the ORIGIN subnet: a cross-netuid call routes through the AMM
    and lands a different amount of ours, so crediting it would pay out on funds that never arrived."""
    assert _verify(_provider(exts=[_ext(origin_netuid=NETUID + 1)])) is None


def test_raw_block_raises_rather_than_reading_as_absent():
    """The raw fallback parses Balances only. Reading it as 'no such payment' would slash a paid leg."""
    p = _provider()
    p.chain.get_block = lambda n: {'extrinsics': [_ext()], '_raw': True}
    with pytest.raises(ProviderUnreachableError):
        _verify(p)


def test_deposit_scan_parks_on_a_raw_block_and_finds_the_deposit_once_it_decodes():
    """A raw block cannot show a stake transfer: the scan must wait on it, never leap past the deposit."""
    p = _provider()
    decoded = p.chain.get_block
    p.chain.get_block = lambda n: {'extrinsics': [], '_raw': True} if n == BLOCK else decoded(n)
    assert p.find_recent_outgoing(USER, MINER, 5_000) is None
    assert p.scan_cursors[(USER, MINER, 5_000)] == BLOCK - 1
    p.chain.get_block = decoded
    assert p.find_recent_outgoing(USER, MINER, 5_000) == TXID


def test_transfer_stake_and_hotkey_settles_on_its_own_event():
    """The sibling call also changes the owning coldkey; only its settlement event differs."""
    ext = _ext(function='transfer_stake_and_hotkey')
    events = [
        _event('SubtensorModule', 'StakeAndHotkeyTransferred'),
        _event('System', 'ExtrinsicSuccess'),
    ]
    info = _verify(_provider(exts=[ext], events=events))
    assert info is not None and info.amount == 5_000


def test_wrong_netuid_or_underpay_do_not_match():
    assert _verify(_provider(exts=[_ext(netuid=NETUID + 1)])) is None
    assert _verify(_provider(exts=[_ext(alpha=4_999)])) is None
    assert _verify(_provider(exts=[_ext(alpha=9_000)])).amount == 9_000


def test_unreadable_events_raise_rather_than_reading_as_absent():
    p = _provider()

    def boom(_):
        raise ProviderUnreachableError('events unavailable')

    p.chain.get_block_events = boom
    with pytest.raises(ProviderUnreachableError):
        _verify(p)


def test_missing_block_time_raises():
    """is_tx_fresh fails closed on None, which would ride a paid leg to a TIMEOUT slash."""
    with pytest.raises(ProviderUnreachableError):
        _provider(block_time=None).verify_transaction(TXID, MINER, 5_000, block_hint=BLOCK)


# ─── balances + price ───────────────────────────────────────────────────────


def _stake(hotkey, rao, netuid=NETUID):
    return SimpleNamespace(hotkey_ss58=hotkey, netuid=netuid, stake=SimpleNamespace(rao=rao))


def test_balance_read_failure_raises_rather_than_reading_as_empty():
    """Unknown is not zero: a swallowed read reports an empty wallet, which drops the
    miner's quotes on a network blip."""

    def boom(_ck):
        raise ConnectionError('subtensor down')

    with pytest.raises(ProviderUnreachableError):
        Alpha(CHAIN_SN7, SimpleNamespace(get_stake_info_for_coldkey=boom)).get_balance(MINER)


# ─── delivery gates ─────────────────────────────────────────────────────────


def _toggles(transfer=True, subtoken=True, exists=True):
    flags = {'TransferToggle': transfer, 'SubtokenEnabled': subtoken, 'NetworksAdded': exists}
    return SimpleNamespace(substrate=SimpleNamespace(query=lambda m, name, params: flags[name]))


def test_transfer_toggle_off_defers_but_never_cancels():
    """The toggle is the subnet owner's to flip at any block, and a cancel leaves the taker's deposit
    with the miner — so an owner running a miner on its own alpha could flip it after every deposit.
    A flip is a deferral hint only; the swap holds and, if transfers never return, times out at the
    extension ceiling exactly like an EVM getCode hint."""
    off = Alpha(CHAIN_SN7, _toggles(transfer=False))
    assert off.cancel_evidence(MINER, 1) is None
    assert off.can_deliver_to(MINER, 1) is False
    assert off.delivery_refused(MINER, 0) is True
    assert Alpha(CHAIN_SN7, _toggles()).delivery_refused(MINER, 0) is False


def test_unreadable_toggle_is_not_evidence_and_defers_the_slash():
    """Reserve fails open (not a security boundary); the slash gate must RAISE — returning False there
    read an RPC failure as "not refused" and let the slash proceed, where every other provider's
    unreadable probe defers it."""

    def boom(*a, **k):
        raise RuntimeError('rpc down')

    p = Alpha(CHAIN_SN7, SimpleNamespace(substrate=SimpleNamespace(query=boom)))
    assert p.can_deliver_to(MINER, 1) is True
    with pytest.raises(ProviderUnreachableError):
        p.delivery_refused(MINER, 0)


# ─── sending ────────────────────────────────────────────────────────────────


class _Wallet:
    coldkeypub = SimpleNamespace(ss58_address=MINER)


def _sender(stakes, *, response=None, calls=None, recipient_hotkeys=(), flags=0, owned=()):
    calls = [] if calls is None else calls
    receipt = SimpleNamespace(extrinsic_hash=TXID, block_hash='0xincl')
    landed = SimpleNamespace(success=True, message='', extrinsic=_ext(), extrinsic_receipt=receipt)

    def transfer_stake(**kwargs):
        calls.append(kwargs)
        return landed if response is None else response

    def sign_and_send_extrinsic(call, **kwargs):
        calls.append(call)
        return landed if response is None else response

    subtensor = SimpleNamespace(
        get_current_block=lambda: HEAD,
        get_stake_info_for_coldkey=lambda ck: stakes,
        transfer_stake=transfer_stake,
        compose_call=lambda module, function, params: {'call_function': function, **params},
        sign_and_send_extrinsic=sign_and_send_extrinsic,
        substrate=SimpleNamespace(
            get_block_number=lambda h: BLOCK,
            query=lambda m, name, params, block_hash=None: {
                'StakingHotkeys': list(recipient_hotkeys),
                'OwnedHotkeys': list(owned),
                'AccountFlags': flags,
            }.get(name, True),
        ),
    )
    p = Alpha(CHAIN_SN7, subtensor, _Wallet())
    p.chain.get_block = lambda n: {'extrinsics': []}
    p.chain.get_block_hash = lambda n: f'0xblock{n}'
    return p, calls


def _payout_lands(p):
    """The chain now shows the miner's settled transfer_stake to the user in BLOCK."""
    p.chain.get_block = lambda n: {'extrinsics': [_ext(dest=USER, sender=MINER)]} if n == BLOCK else {'extrinsics': []}
    p.chain.get_block_events = lambda h: _settled_events()


def test_send_picks_the_largest_hotkey_and_disables_mev_protection():
    p, calls = _sender([_stake('small', 100), _stake('big', 9_000), _stake('other-subnet', 99_999, NETUID + 1)])
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    (call,) = calls
    assert call['hotkey_ss58'] == 'big'
    assert call['mev_protection'] is False
    assert (call['origin_netuid'], call['destination_netuid'], call['destination_coldkey_ss58']) == (
        NETUID,
        NETUID,
        USER,
    )
    assert call['amount'].rao == 5_000


def test_send_reuses_a_prior_broadcast_per_dedup_key():
    """Dedup state lives on this asset, keyed per obligation — never on the shared Tao chain."""
    p, calls = _sender([_stake('hk', 9_000)])
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    _payout_lands(p)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    assert len(calls) == 1
    assert p.send_amount(USER, 5_000, dedup_key='swap-2') == (TXID, BLOCK)
    assert len(calls) == 2
    assert not hasattr(p.chain, 'broadcasted_txids')


def test_ambiguous_response_records_the_attempt_and_the_next_call_adopts_the_landed_send():
    """The SDK folds a mid-submit exception into a hash-less failed response: never re-pay, resolve by content."""
    ambiguous = SimpleNamespace(success=False, message='ws dropped', extrinsic=None, extrinsic_receipt=None)
    p, calls = _sender([_stake('hk', 9_000)], response=ambiguous)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') is None
    assert p.broadcasted_txids['swap-1'] == (USER, 5_000, '', HEAD)
    _payout_lands(p)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    assert len(calls) == 1


def test_whole_position_sentinel_is_not_an_amount():
    """u64::MAX means "my whole live position" on subtensor, so the call's figure is not what moved:
    a dust position would otherwise satisfy any pinned amount (validator and miner both credit >=)."""
    p = Alpha(CHAIN_SN7, SimpleNamespace())
    assert p.decode_transfer_stake(_ext(alpha=2**64 - 1), False) is None
    assert _verify(_provider(exts=[_ext(alpha=2**64 - 1)]), amount=1) is None
    assert _verify(_provider(exts=[_ext(alpha=2**64 - 2)]), amount=1).amount == 2**64 - 2


FULL = [f'hk{i}' for i in range(128)]  # a recipient at subtensor's StakingHotkeys cap


def test_a_recipient_at_the_cap_is_paid_onto_a_hotkey_it_already_stakes_to():
    """A plain transfer_stake would grow a full StakingHotkeys and fail (TooManyStakingHotkeys), so the
    miner lands the stake on one of the recipient's own hotkeys — same netuid, exact, no fee. A full list
    is never undeliverable, so it is never no-fault either."""
    p, calls = _sender([_stake('hk3', 100), _stake('big', 9_000)], recipient_hotkeys=FULL)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    (call,) = calls
    assert call == {
        'call_function': 'transfer_stake_and_hotkey',
        'destination_coldkey': USER,
        'origin_hotkey': 'big',
        'destination_hotkey': 'hk0',
        'origin_netuid': NETUID,
        'destination_netuid': NETUID,
        'alpha_amount': 5_000,
    }
    assert p.cancel_evidence(USER, 5_000, from_address=MINER) is None


def test_a_recipient_refusing_new_hotkeys_is_paid_onto_one_it_already_stakes_to():
    """With the refuse-new-hotkey flag on, landing on the miner's own hotkey fails on-chain; one the
    recipient already stakes to still lands."""
    p, calls = _sender([_stake('big', 9_000)], recipient_hotkeys=['theirs'], flags=1 << 1)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    assert calls[0]['destination_hotkey'] == 'theirs'


def test_a_refused_send_is_returned_as_refusal_evidence():
    """A recipient with no hotkey to land on fails the send on-chain: the failed tx is the evidence."""
    receipt = SimpleNamespace(extrinsic_hash=TXID, block_hash='0xincl')
    refused = SimpleNamespace(
        success=False,
        message="{'name': 'AccountRejectsStakingHotkeyGrowth'}",
        extrinsic=_ext(),
        extrinsic_receipt=receipt,
    )
    p, _ = _sender([_stake('big', 9_000)], response=refused, flags=1 << 1)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)


def _flips_flag_in_the_send_block(recipient_hotkeys=('theirs',), second=None):
    """USER turns its flag on in the same block as the miner's first transfer, which fails on it."""
    state = {'flags': 0}
    receipt = SimpleNamespace(extrinsic_hash=TXID, block_hash='0xincl')
    refused = SimpleNamespace(
        success=False,
        message="{'name': 'AccountRejectsStakingHotkeyGrowth'}",
        extrinsic=_ext(),
        extrinsic_receipt=receipt,
    )
    landed = SimpleNamespace(success=True, message='', extrinsic=_ext(), extrinsic_receipt=receipt)
    responses = [refused, second or landed]
    p, calls = _sender([_stake('big', 9_000)], recipient_hotkeys=recipient_hotkeys)

    def transfer(to_address, origin, destination, amount):
        calls.append((origin, destination))
        state['flags'] = 1 << 1
        return responses.pop(0)

    p.transfer = transfer
    query = p.subtensor.substrate.query
    p.subtensor.substrate.query = lambda m, name, params, block_hash=None: (
        state['flags'] if name == 'AccountFlags' else query(m, name, params, block_hash)
    )
    return p, calls


def test_a_refusal_from_a_flag_flipped_after_the_pick_is_resent_onto_the_recipients_hotkey():
    """The pick saw the flag off and landed on the miner's own hotkey; the recipient turned it on in the
    same block. The failed tx moved nothing, so the miner re-picks and pays onto 'theirs' instead of
    handing in the refusal."""
    p, calls = _flips_flag_in_the_send_block()
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    assert calls == [('big', 'big'), ('big', 'theirs')]


def test_a_resend_is_attempted_at_most_once():
    refused_again = SimpleNamespace(
        success=False,
        message="{'name': 'AccountRejectsStakingHotkeyGrowth'}",
        extrinsic=_ext(),
        extrinsic_receipt=SimpleNamespace(extrinsic_hash=TXID, block_hash='0xincl'),
    )
    p, calls = _flips_flag_in_the_send_block(second=refused_again)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    assert calls == [('big', 'big'), ('big', 'theirs')]


def _refused_on_chain(
    *, error='AccountRejectsStakingHotkeyGrowth', recipient_hotkeys=(), sender=MINER, flags=0, events=(), ahead=0
):
    """The miner's transfer_stake to USER sits in BLOCK behind ``ahead`` other extrinsics and failed with
    ``error``; ``recipient_hotkeys`` and ``flags`` are USER's state at the parent block."""
    p, _ = _sender([], recipient_hotkeys=recipient_hotkeys, flags=flags)
    other = SimpleNamespace(extrinsic_hash=bytes(32), value={'address': USER, 'call': {}})
    exts = [other] * ahead + [_ext(dest=USER, sender=sender)]
    p.chain.get_block = lambda n: {'extrinsics': exts} if n == BLOCK else None
    failed = {'Module': {'index': 7, 'error': '0xaf000000'}}
    refusal = _event('System', 'ExtrinsicFailed', {'dispatch_error': failed}, ahead)
    p.chain.get_block_events = lambda h: [*events, refusal]
    p.subtensor.substrate.metadata = SimpleNamespace(
        get_module_error=lambda module_index, error_index: SimpleNamespace(name=error)
    )
    return p


def _flag_set(who=USER, idx=0, enabled=True):
    """The SubtensorModule event set_reject_staking_hotkey_growth emits, in extrinsic ``idx``."""
    return _event('SubtensorModule', 'RejectStakingHotkeyGrowthUpdated', {'coldkey': who, 'enabled': enabled}, idx)


def test_a_refused_send_to_a_recipient_with_no_hotkey_is_cancel_evidence():
    p = _refused_on_chain()
    assert p.cancel_evidence(USER, 5_000, TXID, from_address=MINER, block_hint=BLOCK) == 8
    assert p.cancel_evidence(USER, 5_000, TXID, from_address=MINER) is None


@pytest.mark.parametrize(
    'case',
    [
        dict(recipient_hotkeys=['theirs']),  # the miner could have landed there
        dict(recipient_hotkeys=['theirs'], flags=1 << 1),  # flag already on: it should have
        dict(error='SlippageTooHigh'),
        dict(sender='someoneElse'),
    ],
)
def test_a_refusal_the_miner_could_have_avoided_is_not_cancel_evidence(case):
    p = _refused_on_chain(**case)
    assert p.cancel_evidence(USER, 5_000, TXID, from_address=MINER, block_hint=BLOCK) is None


def test_a_flag_the_recipient_turned_on_in_the_delivery_block_is_cancel_evidence():
    """At the parent block the flag was off, so landing on the miner's own hotkey was legal; the taker
    then turned the flag on ahead of the miner's transfer in the same block. The miner did nothing it
    could have avoided, so the failed transfer is a no-fault cancel, not a slash."""
    p = _refused_on_chain(recipient_hotkeys=['theirs'], events=[_flag_set(idx=0)], ahead=1)
    assert p.cancel_evidence(USER, 5_000, TXID, from_address=MINER, block_hint=BLOCK) == 8


@pytest.mark.parametrize(
    'case',
    [
        # the flag was already on at the parent block: the miner should have landed on 'theirs'
        dict(flags=1 << 1, events=[_flag_set(idx=0)]),
        # no flag change in the block: a coldkey swap copies AccountFlags without this event
        dict(events=[]),
        # turned on by someone else, or only after the miner's transfer
        dict(events=[_flag_set(who='someoneElse', idx=0)]),
        dict(events=[_flag_set(idx=1)]),
    ],
)
def test_a_same_block_refusal_counts_only_when_the_recipient_itself_flipped_the_flag(case):
    p = _refused_on_chain(**{'recipient_hotkeys': ['theirs'], 'ahead': 1, **case})
    assert p.cancel_evidence(USER, 5_000, TXID, from_address=MINER, block_hint=BLOCK) is None


def test_refusal_evidence_never_reads_head_state_for_a_missing_parent_block():
    p = _refused_on_chain(recipient_hotkeys=['theirs'], events=[_flag_set(idx=0)], ahead=1)
    p.chain.get_block_hash = lambda n: None if n == BLOCK - 1 else f'0xblock{n}'
    with pytest.raises(ProviderUnreachableError):
        p.cancel_evidence(USER, 5_000, TXID, from_address=MINER, block_hint=BLOCK)


def test_send_needs_one_hotkey_holding_the_whole_amount():
    """get_balance sums across hotkeys, but the chain debits ONE position: a summed balance passed the
    miner's inventory gate while the send failed every pass and rode to a slash."""
    p, calls = _sender([_stake('a', 3_000), _stake('b', 3_000)])
    assert p.get_balance(MINER) == 6_000
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') is None
    assert calls == []


def test_a_split_stake_is_blocked_before_reserve_with_the_largest_position_named():
    """A taker's alpha split across hotkeys cannot go out as one transfer_stake; two partial sends each
    fail the amount match and strand the deposit with the miner — so refuse the swap up front."""
    p, _ = _sender([_stake('a', 6 * 10**9), _stake('b', 6 * 10**9)])
    assert p.send_blocker(USER, MINER, 10 * 10**9) == (
        'SN7 must go out as one transfer_stake from one hotkey; your largest position holds 6 of the 10 needed'
        ' — move it onto one hotkey first'
    )
    assert p.send_blocker(USER, MINER, 5 * 10**9) is None


def test_an_unreadable_stake_does_not_block_the_swap():
    def boom(ck):
        raise ConnectionError('rpc down')

    assert Alpha(CHAIN_SN7, SimpleNamespace(get_stake_info_for_coldkey=boom)).send_blocker(USER, MINER, 1) is None


def test_locked_alpha_the_sender_cannot_move_blocks_the_reservation():
    """A send past the lock-free amount carries the lock and fails at a default recipient: refuse it up front."""
    p, _ = _sender([_stake('big', 9 * 10**9)])
    p.subtensor.substrate.runtime_call = lambda api, method, params: {params[0][0]: {NETUID: {'available': 4 * 10**9}}}
    assert p.send_blocker(USER, MINER, 5 * 10**9) == (
        'only 4 of your SN7 is free to send (the rest is locked); swap that much or less'
    )
    assert p.send_blocker(USER, MINER, 4 * 10**9) is None

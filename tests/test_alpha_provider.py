"""Alpha settlement: a top-level transfer_stake that succeeded AND emitted StakeTransferred, amount from the CALL."""

from types import SimpleNamespace

import pytest

from allways.assets import ASSET_REGISTRY
from allways.assets.alpha import Alpha
from allways.assets.asset import ProviderUnreachableError
from allways.assets.tao import BETA_ESCROW, Tao
from allways.chains import ALPHA_NETUIDS, CHAIN_SN7, CHAIN_SN74

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


def test_alphas_are_registered_and_bind_the_tao_chain():
    ids = {spec.chain_id: spec for spec in ASSET_REGISTRY}
    # Every netuid is registered, each bound to its own ChainDefinition and nothing else.
    assert {f'sn{n}' for n in ALPHA_NETUIDS} <= set(ids)
    assert ids['sn7'].cls.func is Alpha and ids['sn7'].cls.args == (CHAIN_SN7,)
    assert ids['sn74'].cls.args == (CHAIN_SN74,)
    assert ids['sn7'].kwarg_names == ids['tao'].kwarg_names
    p = Alpha(CHAIN_SN7, SimpleNamespace())
    assert isinstance(p, Alpha) and isinstance(p.chain, Tao) and p.netuid == 7
    assert Alpha(CHAIN_SN74, SimpleNamespace()).netuid == 74


def test_beta_escrow_is_never_a_valid_payee_on_tao_or_alpha():
    # Keyless protocol custody: transfer_stake into it fails (CannotUseSystemAccount), TAO sent there is
    # stranded. Invalid at the chain, so reserve refuses it and an in-flight swap cancels no-fault.
    assert BETA_ESCROW == '5EYCAe5jLQhn6ofDSwHx3AZmsZPVFHnKpstqap4vqwDWtp7s'
    for chain in (Tao(SimpleNamespace()), Alpha(CHAIN_SN7, SimpleNamespace()).chain):
        assert not chain.is_valid_address(BETA_ESCROW)
        assert chain.is_valid_address('5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY')


# ─── verification ───────────────────────────────────────────────────────────


def test_real_extrinsic_shape_decodes_with_its_hash():
    """substrate.get_block yields GenericExtrinsic objects: the hash lives on the object, not in .value."""
    assert Alpha(CHAIN_SN7, SimpleNamespace()).decode_transfer_stake(_ext(), False) == (TXID, MINER, 5_000, USER)


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
        _verify(_provider(block_time=None))


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


def test_get_balance_sums_this_netuid_across_hotkeys():
    stakes = [_stake('hk1', 100), _stake('hk2', 250), _stake('hk3', 999, netuid=NETUID + 1)]
    assert Alpha(CHAIN_SN7, SimpleNamespace(get_stake_info_for_coldkey=lambda ck: stakes)).get_balance(MINER) == 350


def test_value_rao_floors_and_raises_on_failure():
    p = Alpha(CHAIN_SN7, SimpleNamespace(get_subnet_price=lambda netuid, block=None: SimpleNamespace(rao=333_333_333)))
    assert p.value_rao(3) == 0
    assert p.value_rao(3_000_000_000) == 999_999_999

    def boom(netuid, block=None):
        raise RuntimeError('rpc down')

    with pytest.raises(ProviderUnreachableError):
        Alpha(CHAIN_SN7, SimpleNamespace(get_subnet_price=boom)).value_rao(1)

    calls = []

    def historical(netuid, block=None):
        calls.append((netuid, block))
        return SimpleNamespace(rao=2 * 10**9)

    assert Alpha(CHAIN_SN7, SimpleNamespace(get_subnet_price=historical)).value_rao(3 * 10**9, block=42) == 6 * 10**9
    assert calls == [(NETUID, 42)]


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


def test_a_pruned_subnet_defers_and_never_cancels():
    """SubnetLimit is full, so a registration dissolves the lowest-priced subnet and its alpha is
    force-liquidated to coldkey TAO. A no-fault cancel would leave the taker's deposit with the miner,
    so the swap defers and times out: the vault pays the taker, same as transfers-off."""
    gone = Alpha(CHAIN_SN7, _toggles(exists=False))
    assert gone.can_deliver_to(MINER, 1) is False
    assert gone.delivery_refused(MINER, 0) is True
    assert gone.cancel_evidence(MINER, 1) is None


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


def _sender(stakes, *, response=None, calls=None, recipient_hotkeys=()):
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
            query=lambda m, name, params: list(recipient_hotkeys) if name == 'StakingHotkeys' else True,
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
    assert not p.chain.broadcasted_txids


def test_ambiguous_response_records_the_attempt_and_the_next_call_adopts_the_landed_send():
    """The SDK folds a mid-submit exception into a hash-less failed response: never re-pay, resolve by content."""
    ambiguous = SimpleNamespace(success=False, message='ws dropped', extrinsic=None, extrinsic_receipt=None)
    p, calls = _sender([_stake('hk', 9_000)], response=ambiguous)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') is None
    assert p.broadcasted_txids['swap-1'] == (USER, 5_000, '', HEAD)
    _payout_lands(p)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    assert len(calls) == 1


def test_failed_response_with_a_signed_extrinsic_keeps_its_hash():
    """A submit that raised after signing still names the extrinsic: the next poll probes that exact hash."""
    signed_only = SimpleNamespace(success=False, message='ws dropped', extrinsic=_ext(), extrinsic_receipt=None)
    p, _ = _sender([_stake('hk', 9_000)], response=signed_only)
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') is None
    assert p.broadcasted_txids['swap-1'][2] == TXID


def test_whole_position_sentinel_is_not_an_amount():
    """u64::MAX means "my whole live position" on subtensor, so the call's figure is not what moved:
    a dust position would otherwise satisfy any pinned amount (validator and miner both credit >=)."""
    p = Alpha(CHAIN_SN7, SimpleNamespace())
    assert p.decode_transfer_stake(_ext(alpha=2**64 - 1), False) is None
    assert _verify(_provider(exts=[_ext(alpha=2**64 - 1)]), amount=1) is None
    assert _verify(_provider(exts=[_ext(alpha=2**64 - 2)]), amount=1).amount == 2**64 - 2


FULL = [f'hk{i}' for i in range(128)]  # a recipient at subtensor's StakingHotkeys cap


def test_send_lands_on_a_hotkey_the_recipient_already_stakes_to():
    """A transfer that lands on a hotkey the recipient already holds never grows its StakingHotkeys,
    so it can never hit the cap — preferred even over a larger position."""
    p, calls = _sender([_stake('small', 6_000), _stake('big', 9_000)], recipient_hotkeys=['small'])
    assert p.send_amount(USER, 5_000, dedup_key='swap-1') == (TXID, BLOCK)
    assert calls[0]['hotkey_ss58'] == 'small'


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

"""Alpha settlement: a top-level transfer_stake that succeeded AND emitted StakeTransferred, amount from the CALL."""

from types import SimpleNamespace

import pytest

from allways.assets import ASSET_REGISTRY, Sn7, Sn74
from allways.assets.alpha import Alpha
from allways.assets.asset import ProviderUnreachableError
from allways.assets.tao import Tao
from allways.chains import CHAIN_SN7
from allways.constants import CANCEL_REASON_ALPHA_TRANSFER_DISABLED

MINER = 'minerCold'
USER = 'userCold'
HOTKEY = 'hotkeyA'
TXID_BYTES = bytes.fromhex('ab' * 32)
TXID = '0x' + TXID_BYTES.hex()
BLOCK = 500
HEAD = BLOCK + 10
NETUID = CHAIN_SN7.netuid


def _ext(alpha=5_000, netuid=NETUID, dest=MINER, sender=USER, module='SubtensorModule', function='transfer_stake'):
    """The real substrate.get_block shape: a GenericExtrinsic with bytes .extrinsic_hash and a .value dict."""
    value = {
        'address': sender,
        'call': {
            'call_module': module,
            'call_function': function,
            'call_args': [
                {'name': 'destination_coldkey', 'value': dest},
                {'name': 'hotkey', 'value': HOTKEY},
                {'name': 'origin_netuid', 'value': netuid},
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
    p = Sn7(SimpleNamespace(get_current_block=lambda: HEAD), wallet)
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
    assert ids['sn7'].cls is Sn7 and ids['sn74'].cls is Sn74
    assert ids['sn7'].kwarg_names == ids['tao'].kwarg_names
    p = Sn7(SimpleNamespace())
    assert isinstance(p, Alpha) and isinstance(p.chain, Tao) and p.netuid == 7
    assert Sn74(SimpleNamespace()).netuid == 74


# ─── verification ───────────────────────────────────────────────────────────


def test_real_extrinsic_shape_decodes_with_its_hash():
    """substrate.get_block yields GenericExtrinsic objects: the hash lives on the object, not in .value."""
    assert Sn7(SimpleNamespace()).decode_transfer_stake(_ext(), False) == (TXID, MINER, 5_000, USER)


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


def test_get_balance_sums_this_netuid_across_hotkeys():
    stakes = [_stake('hk1', 100), _stake('hk2', 250), _stake('hk3', 999, netuid=NETUID + 1)]
    assert Sn7(SimpleNamespace(get_stake_info_for_coldkey=lambda ck: stakes)).get_balance(MINER) == 350


def test_value_rao_floors_and_raises_on_failure():
    p = Sn7(SimpleNamespace(get_subnet_price=lambda netuid: SimpleNamespace(rao=333_333_333)))
    assert p.value_rao(3) == 0
    assert p.value_rao(3_000_000_000) == 999_999_999

    def boom(netuid):
        raise RuntimeError('rpc down')

    with pytest.raises(ProviderUnreachableError):
        Sn7(SimpleNamespace(get_subnet_price=boom)).value_rao(1)


# ─── delivery gates ─────────────────────────────────────────────────────────


def _toggles(transfer=True, subtoken=True):
    flags = {'TransferToggle': transfer, 'SubtokenEnabled': subtoken}
    return SimpleNamespace(substrate=SimpleNamespace(query=lambda m, name, params: flags[name]))


def test_cancel_evidence_on_transfer_toggle_off():
    assert Sn7(_toggles(transfer=False)).cancel_evidence(MINER, 1) == CANCEL_REASON_ALPHA_TRANSFER_DISABLED
    assert Sn7(_toggles(subtoken=False)).cancel_evidence(MINER, 1) == CANCEL_REASON_ALPHA_TRANSFER_DISABLED
    assert Sn7(_toggles()).cancel_evidence(MINER, 1) is None
    assert Sn7(_toggles(transfer=False)).can_deliver_to(MINER, 1) is False
    assert Sn7(_toggles(transfer=False)).delivery_refused(MINER, 0) is True


def test_unreadable_toggle_is_not_evidence():
    def boom(*a, **k):
        raise RuntimeError('rpc down')

    p = Sn7(SimpleNamespace(substrate=SimpleNamespace(query=boom)))
    assert p.can_deliver_to(MINER, 1) is True
    assert p.delivery_refused(MINER, 0) is False
    assert p.cancel_evidence(MINER, 1) is None


# ─── sending ────────────────────────────────────────────────────────────────


class _Wallet:
    coldkeypub = SimpleNamespace(ss58_address=MINER)


def _sender(stakes, *, response=None, calls=None):
    calls = [] if calls is None else calls
    receipt = SimpleNamespace(extrinsic_hash=TXID, block_hash='0xincl')
    landed = SimpleNamespace(success=True, message='', extrinsic=_ext(), extrinsic_receipt=receipt)

    def transfer_stake(**kwargs):
        calls.append(kwargs)
        return landed if response is None else response

    subtensor = SimpleNamespace(
        get_current_block=lambda: HEAD,
        get_stake_info_for_coldkey=lambda ck: stakes,
        transfer_stake=transfer_stake,
        substrate=SimpleNamespace(get_block_number=lambda h: BLOCK),
    )
    p = Sn7(subtensor, _Wallet())
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

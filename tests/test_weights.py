import pytest

from allways import constants
from allways.weights import select_target, weight_vector

TREASURY = constants.TREASURY_HOTKEY
OWNER = 'owner-hotkey'
OWNER_UID = 3


def hotkeys(n=8, treasury_at=None, owner_at=OWNER_UID):
    keys = [f'hotkey-{uid}' for uid in range(n)]
    if owner_at is not None:
        keys[owner_at] = OWNER
    if treasury_at is not None:
        keys[treasury_at] = TREASURY
    return keys


def select(keys, treasury_uid, netuid=7, owner=OWNER):
    return select_target(keys, netuid, 7, treasury_uid, TREASURY, owner)


def test_treasury_uid_with_matching_hotkey_is_the_target():
    assert select(hotkeys(treasury_at=5), 5) == (5, None)


def test_unset_uid_burns_to_the_owner_uid():
    uid, reason = select(hotkeys(treasury_at=5), None)
    assert uid == OWNER_UID
    assert 'unset' in reason


@pytest.mark.parametrize('bad_uid', [-1, 8, 4096])
def test_out_of_range_uid_burns(bad_uid):
    uid, reason = select(hotkeys(treasury_at=5), bad_uid)
    assert uid == OWNER_UID
    assert 'out of range' in reason


def test_recycled_slot_burns_instead_of_paying_a_stranger():
    uid, reason = select(hotkeys(), 5)
    assert uid == OWNER_UID
    assert 'hotkey mismatch' in reason
    assert 'hotkey-5' in reason


def test_treasury_hotkey_registered_at_another_uid_still_burns():
    # The UID is what is pinned: the hotkey is a guard, never a lookup.
    uid, reason = select(hotkeys(treasury_at=6), 5)
    assert uid == OWNER_UID
    assert 'hotkey mismatch' in reason


@pytest.mark.parametrize('not_a_uid', [True, '5', 5.0])
def test_non_integer_uid_burns(not_a_uid):
    uid, reason = select(hotkeys(treasury_at=5), not_a_uid)
    assert uid == OWNER_UID
    assert reason


def test_another_netuid_never_uses_the_treasury_constants():
    # Even with the treasury hotkey sitting at the pinned UID: the constants are mainnet values.
    uid, reason = select(hotkeys(treasury_at=5), 5, netuid=19)
    assert uid == OWNER_UID
    assert 'netuid 19' in reason


def test_burn_uid_follows_the_owner_hotkey():
    assert select(hotkeys(owner_at=6), None)[0] == 6


@pytest.mark.parametrize('owner', [OWNER, None, ''])
def test_owner_hotkey_missing_means_no_target_at_all(owner):
    uid, reason = select(hotkeys(owner_at=None), None, owner=owner)
    assert uid is None
    assert 'not registered' in reason
    assert 'unset' in reason


def test_treasury_is_still_used_when_the_owner_hotkey_is_missing():
    assert select(hotkeys(treasury_at=5, owner_at=None), 5) == (5, None)


def test_weight_vector_puts_everything_on_one_uid():
    uids, weights = weight_vector(7)
    assert uids == [7]
    assert weights == [1.0]
    assert sum(weights) == 1.0


def test_shipped_constants_are_well_formed():
    assert constants.NETUID_FINNEY == 7
    assert constants.TREASURY_UID is None or (
        type(constants.TREASURY_UID) is int and 0 <= constants.TREASURY_UID < 4096
    )
    assert not hasattr(constants, 'BURN_UID')
    assert len(constants.TREASURY_HOTKEY) == 48 and constants.TREASURY_HOTKEY.startswith('5')

import pytest

from allways import constants
from allways.weights import select_target, weight_vector

TREASURY = constants.TREASURY_HOTKEY
BURN = 3


def hotkeys(n=8, treasury_at=None):
    keys = [f'hotkey-{uid}' for uid in range(n)]
    if treasury_at is not None:
        keys[treasury_at] = TREASURY
    return keys


def test_treasury_uid_with_matching_hotkey_is_the_target():
    assert select_target(hotkeys(treasury_at=5), 5, TREASURY, BURN) == (5, None)


def test_unset_uid_burns():
    uid, reason = select_target(hotkeys(treasury_at=5), None, TREASURY, BURN)
    assert uid == BURN
    assert 'unset' in reason


@pytest.mark.parametrize('bad_uid', [-1, 8, 4096])
def test_out_of_range_uid_burns(bad_uid):
    uid, reason = select_target(hotkeys(treasury_at=5), bad_uid, TREASURY, BURN)
    assert uid == BURN
    assert 'out of range' in reason


def test_recycled_slot_burns_instead_of_paying_a_stranger():
    uid, reason = select_target(hotkeys(), 5, TREASURY, BURN)
    assert uid == BURN
    assert 'hotkey mismatch' in reason
    assert 'hotkey-5' in reason


def test_treasury_hotkey_registered_at_another_uid_still_burns():
    # The UID is what is pinned: the hotkey is a guard, never a lookup.
    uid, reason = select_target(hotkeys(treasury_at=6), 5, TREASURY, BURN)
    assert uid == BURN
    assert 'hotkey mismatch' in reason


@pytest.mark.parametrize('not_a_uid', [True, '5', 5.0])
def test_non_integer_uid_burns(not_a_uid):
    uid, reason = select_target(hotkeys(treasury_at=5), not_a_uid, TREASURY, BURN)
    assert uid == BURN
    assert reason


def test_burn_uid_beyond_the_subnet_falls_back_to_uid_zero():
    assert select_target(hotkeys(n=4), None, TREASURY, 53)[0] == 0


def test_treasury_may_sit_on_the_burn_uid_slot():
    assert select_target(hotkeys(treasury_at=BURN), BURN, TREASURY, BURN) == (BURN, None)


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
    assert type(constants.BURN_UID) is int and constants.BURN_UID >= 0
    assert len(constants.TREASURY_HOTKEY) == 48 and constants.TREASURY_HOTKEY.startswith('5')

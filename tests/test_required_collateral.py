from allways.constants import required_collateral


def test_rounds_up_like_the_contract():
    # The 3.8.1 suite's TAO-backed timeout: collateral 103000001 rao, the program's verdict named a
    # 113300002 penalty (ceil-div in constants.rs). A floor said 113300001.
    assert required_collateral(103_000_001) == 113_300_002


def test_a_whole_requirement_is_unchanged():
    assert required_collateral(103_000_000) == 113_300_000
    assert required_collateral(0) == 0

// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {AllwaysUsdcVault} from "../src/AllwaysUsdcVault.sol";
import {VaultTestBase} from "./VaultTestBase.sol";

/// System-swap lot payments from the pot. Three validators at 66%: two votes carry a lot.
contract PayLotTest is VaultTestBase {
    bytes32 internal constant LOT = bytes32(uint256(0x107));
    uint256 internal constant AMT = 500 * USDC;
    uint64 internal dl;

    function setUp() public override {
        super.setUp();
        vault = _deploy(_set3(v1, v2, v3), 66);
        _post(bob, 1000 * USDC);
        _lock(bob);
        usdc.mint(address(vault), 2000 * USDC); // pot
        dl = uint64(block.timestamp + 1800);
    }

    function _vote(address val, bytes32 ref, address m, uint256 amt, uint64 deadline) internal {
        vm.prank(val);
        vault.votePayLot(ref, m, amt, deadline);
    }

    function _expectVote(address val, bytes memory err, bytes32 ref, address m, uint256 amt, uint64 deadline)
        internal
    {
        vm.prank(val);
        vm.expectRevert(err);
        vault.votePayLot(ref, m, amt, deadline);
    }

    function _status(bytes32 ref) internal view returns (AllwaysUsdcVault.LotStatus) {
        return vault.lotStatus(ref);
    }

    function test_PayLotPaysFromPotToBondAddress() public {
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.LotOpened(LOT, bob, AMT, dl, v1);
        _vote(v1, LOT, bob, AMT, dl);
        (bytes32 openRef, uint64 openDl) = vault.openLot();
        assertEq(openRef, LOT);
        assertEq(openDl, dl);
        assertTrue(vault.hasVoted(vault.roundKeyForPayLot(), v1));

        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.LotPaid(LOT, bob, AMT, 1500 * USDC);
        _vote(v2, LOT, bob, AMT, dl);
        assertEq(usdc.balanceOf(bob), AMT);
        assertEq(uint8(_status(LOT)), uint8(AllwaysUsdcVault.LotStatus.Paid));
        assertEq(vault.totalLotsPaid(), AMT);
        assertEq(vault.pot(), 1500 * USDC);
        (openRef,) = vault.openLot();
        assertEq(openRef, bytes32(0));
    }

    function test_PayLotChecksPotOnEveryVote() public {
        usdc.wipe(address(vault), 1600 * USDC); // pot = 400
        _expectVote(
            v1,
            abi.encodeWithSelector(AllwaysUsdcVault.LotExceedsPot.selector, AMT, 400 * USDC),
            LOT,
            bob,
            AMT,
            dl
        );
        // The opening vote passes; the pot then shrinks and the next vote is refused.
        _vote(v1, LOT, bob, 300 * USDC, dl);
        usdc.wipe(address(vault), 200 * USDC);
        _expectVote(
            v2,
            abi.encodeWithSelector(AllwaysUsdcVault.LotExceedsPot.selector, 300 * USDC, 200 * USDC),
            LOT,
            bob,
            300 * USDC,
            dl
        );
        assertEq(usdc.balanceOf(bob), 0);
    }

    function test_PayLotRechecksPotAtQuorumAfterSlashFallback() public {
        _vote(v1, LOT, bob, AMT, dl);

        // A parked slash between votes does not shrink the pot (the bond it seizes was owed too)…
        uint256 potBefore = vault.pot();
        usdc.blacklist(user);
        _post(carol, 1000 * USDC);
        bytes32 ref = bytes32(uint256(1));
        _slash(v1, carol, ref, 400 * USDC, user, 400 * USDC);
        _slash(v2, carol, ref, 400 * USDC, user, 400 * USDC);
        assertEq(vault.pendingSlashTotal(), 400 * USDC);
        assertEq(vault.pot(), potBefore);

        // …but anything that does shrink it is caught by the quorum vote's own guard.
        usdc.wipe(address(vault), 1600 * USDC);
        _expectVote(
            v2,
            abi.encodeWithSelector(AllwaysUsdcVault.LotExceedsPot.selector, AMT, 400 * USDC),
            LOT,
            bob,
            AMT,
            dl
        );
        assertEq(uint8(_status(LOT)), uint8(AllwaysUsdcVault.LotStatus.None));
        assertEq(usdc.balanceOf(bob), 0);

        // A parked slash on the recipient itself drops its coverage, and the quorum vote refuses.
        usdc.mint(address(vault), 1600 * USDC);
        bytes32 ref2 = bytes32(uint256(2));
        _slash(v1, bob, ref2, 600 * USDC, user, 600 * USDC);
        _slash(v2, bob, ref2, 600 * USDC, user, 600 * USDC);
        _expectVote(
            v2, abi.encodeWithSelector(AllwaysUsdcVault.RecipientUnderCovered.selector), LOT, bob, AMT, dl
        );
        assertEq(usdc.balanceOf(bob), 0);
    }

    function test_PayLotOnlyOneOpen() public {
        _post(carol, 1000 * USDC);
        _lock(carol);
        bytes32 other = bytes32(uint256(0x108));
        _vote(v1, LOT, bob, AMT, dl);
        _expectVote(
            v2, abi.encodeWithSelector(AllwaysUsdcVault.LotAlreadyOpen.selector, LOT), other, carol, AMT, dl
        );
        // A different tuple for the same lot is also a different round.
        _expectVote(
            v2, abi.encodeWithSelector(AllwaysUsdcVault.LotAlreadyOpen.selector, LOT), LOT, bob, AMT - 1, dl
        );
        _vote(v2, LOT, bob, AMT, dl);
        // Once it settles, the slot is free.
        _vote(v1, other, carol, AMT, dl);
        (bytes32 openRef,) = vault.openLot();
        assertEq(openRef, other);
    }

    function test_PayLotDeadOpenLotIsReplaceable() public {
        _post(carol, 1000 * USDC);
        _lock(carol);
        bytes32 other = bytes32(uint256(0x108));

        // Dead by deadline.
        uint64 shortDl = uint64(block.timestamp + 60);
        _vote(v1, LOT, bob, AMT, shortDl);
        skip(61);
        _expectVote(
            v2, abi.encodeWithSelector(AllwaysUsdcVault.LotDeadlinePassed.selector), LOT, bob, AMT, shortDl
        );
        uint64 dl2 = uint64(block.timestamp + 1800);
        _vote(v2, other, carol, AMT, dl2);
        (bytes32 openRef,) = vault.openLot();
        assertEq(openRef, other);
        assertFalse(vault.hasVoted(vault.roundKeyForPayLot(), v1), "old round cleared");

        // Dead by round TTL while the deadline still holds.
        uint64 dl3 = uint64(block.timestamp + 3600);
        skip(1801);
        _vote(v3, LOT, bob, AMT, dl3);
        (openRef,) = vault.openLot();
        assertEq(openRef, LOT);
        _vote(v1, LOT, bob, AMT, dl3);
        assertEq(uint8(_status(LOT)), uint8(AllwaysUsdcVault.LotStatus.Paid));
        assertEq(uint8(_status(other)), uint8(AllwaysUsdcVault.LotStatus.None));
    }

    function test_PayLotRecipientMustBeLockedBonded() public {
        _post(carol, 1000 * USDC);
        _expectVote(
            v1, abi.encodeWithSelector(AllwaysUsdcVault.RecipientNotBonded.selector), LOT, carol, AMT, dl
        );
        _expectVote(
            v1, abi.encodeWithSelector(AllwaysUsdcVault.RecipientNotBonded.selector), LOT, frank, AMT, dl
        );

        // Locked, but below a raised floor.
        _lock(carol);
        AllwaysUsdcVault.Config memory cfg = _cfg(66);
        cfg.minCollateral = 2000e6;
        vm.prank(v1);
        vault.voteSetConfig(cfg);
        vm.prank(v2);
        vault.voteSetConfig(cfg);
        vm.prank(v3);
        vault.voteSetConfig(cfg);
        _expectVote(
            v1, abi.encodeWithSelector(AllwaysUsdcVault.RecipientNotBonded.selector), LOT, carol, AMT, dl
        );
    }

    function test_PayLotRecipientCoverage110() public {
        // 1000 USDC bond covers lots up to 909.09 USDC.
        _expectVote(
            v1,
            abi.encodeWithSelector(AllwaysUsdcVault.RecipientUnderCovered.selector),
            LOT,
            bob,
            909_090_910,
            dl
        );
        _vote(v1, LOT, bob, 909_090_909, dl);
        _vote(v2, LOT, bob, 909_090_909, dl);
        assertEq(usdc.balanceOf(bob), 909_090_909);
    }

    function test_PayLotNeverTouchesBonds() public {
        uint256 tc = vault.totalCollateral();
        _vote(v1, LOT, bob, AMT, dl);
        _vote(v2, LOT, bob, AMT, dl);
        assertEq(_collateral(bob), 1000 * USDC);
        assertEq(vault.totalCollateral(), tc);
        _assertBooks();
    }

    function test_PayLotOncePerRef() public {
        _vote(v1, LOT, bob, AMT, dl);
        _vote(v2, LOT, bob, AMT, dl);
        _expectVote(
            v3, abi.encodeWithSelector(AllwaysUsdcVault.LotAlreadySettled.selector), LOT, bob, AMT, dl
        );
        _expectVote(
            v1, abi.encodeWithSelector(AllwaysUsdcVault.LotAlreadySettled.selector), LOT, bob, 1 * USDC, dl
        );
        assertEq(usdc.balanceOf(bob), AMT);
    }

    function test_PayLotRejectsZeroRef() public {
        _expectVote(
            v1, abi.encodeWithSelector(AllwaysUsdcVault.InvalidLotRef.selector), bytes32(0), bob, AMT, dl
        );
    }

    function test_PayLotCapAndFloor() public {
        _post(bob, 2000 * USDC);
        bytes memory capErr = abi.encodeWithSelector(AllwaysUsdcVault.LotExceedsCap.selector);
        _expectVote(v1, capErr, LOT, bob, 1 * USDC - 1, dl);
        _expectVote(v1, capErr, LOT, bob, 1000 * USDC + 1, dl);
        _vote(v1, LOT, bob, 1000 * USDC, dl);
        _vote(v2, LOT, bob, 1000 * USDC, dl);
        bytes32 lot2 = bytes32(uint256(0x109));
        _vote(v1, lot2, bob, 1 * USDC, dl);
        _vote(v2, lot2, bob, 1 * USDC, dl);
        assertEq(usdc.balanceOf(bob), 1001 * USDC);
    }

    function test_PayLotDeadlinePassedRefused() public {
        _expectVote(
            v1,
            abi.encodeWithSelector(AllwaysUsdcVault.LotDeadlinePassed.selector),
            LOT,
            bob,
            AMT,
            uint64(block.timestamp - 1)
        );
        _vote(v1, LOT, bob, AMT, uint64(block.timestamp));
    }

    function test_PayLotDeadlineTooFarRefused() public {
        _expectVote(
            v1,
            abi.encodeWithSelector(AllwaysUsdcVault.LotDeadlineTooFar.selector),
            LOT,
            bob,
            AMT,
            uint64(block.timestamp + 3601)
        );
        _vote(v1, LOT, bob, AMT, uint64(block.timestamp + 3600));
    }

    function test_PayLotPushFailureLeavesFundsInPot() public {
        usdc.blacklist(bob);
        uint256 potBefore = vault.pot();
        _vote(v1, LOT, bob, AMT, dl);
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.LotPayFailed(LOT, bob, AMT);
        _vote(v2, LOT, bob, AMT, dl);
        assertEq(uint8(_status(LOT)), uint8(AllwaysUsdcVault.LotStatus.Failed));
        assertEq(vault.pot(), potBefore);
        assertEq(vault.totalLotsPaid(), 0);
        assertEq(vault.pendingSlashTotal(), 0, "no parked lot claim");
        (bytes32 openRef,) = vault.openLot();
        assertEq(openRef, bytes32(0));
        // The ref is consumed for good; the slot is free for the next lot.
        usdc.unBlacklist(bob);
        _expectVote(
            v3, abi.encodeWithSelector(AllwaysUsdcVault.LotAlreadySettled.selector), LOT, bob, AMT, dl
        );
    }

    function test_PayLotHashBindsAllArgs() public {
        _post(carol, 1000 * USDC);
        _lock(carol);
        _vote(v1, LOT, bob, AMT, dl);
        bytes memory open = abi.encodeWithSelector(AllwaysUsdcVault.LotAlreadyOpen.selector, LOT);
        _expectVote(v2, open, LOT, carol, AMT, dl);
        _expectVote(v2, open, LOT, bob, AMT + 1, dl);
        _expectVote(v2, open, LOT, bob, AMT, dl + 1);
        assertEq(uint8(_status(LOT)), uint8(AllwaysUsdcVault.LotStatus.None));
        assertEq(usdc.balanceOf(bob), 0);
    }

    function test_PayLotFailedRecheckPaysNothing() public {
        _vote(v1, LOT, bob, AMT, dl);
        // The recipient's bond unlocks between votes.
        vm.prank(v1);
        vault.voteUnlock(bob, 1);
        vm.prank(v2);
        vault.voteUnlock(bob, 1);
        uint256 bal = usdc.balanceOf(address(vault));
        _expectVote(
            v2, abi.encodeWithSelector(AllwaysUsdcVault.RecipientNotBonded.selector), LOT, bob, AMT, dl
        );
        _expectVote(
            v3, abi.encodeWithSelector(AllwaysUsdcVault.RecipientNotBonded.selector), LOT, bob, AMT, dl
        );
        assertEq(usdc.balanceOf(address(vault)), bal);
        assertEq(uint8(_status(LOT)), uint8(AllwaysUsdcVault.LotStatus.None));
        // The round stays open with its one vote until the TTL or deadline frees it.
        (bytes32 openRef,) = vault.openLot();
        assertEq(openRef, LOT);
        assertTrue(vault.hasVoted(vault.roundKeyForPayLot(), v1));
    }

    function test_PayLotLedgerSaturates() public {
        // accumulatedFees is 0 (the pot came from a donation); paying saturates it at 0.
        assertEq(vault.accumulatedFees(), 0);
        _vote(v1, LOT, bob, AMT, dl);
        _vote(v2, LOT, bob, AMT, dl);
        assertEq(vault.accumulatedFees(), 0);
        // With booked fees, the ledger decreases by the lot.
        AllwaysUsdcVault.FeeEntry[] memory e = _entries1(bob, 800 * USDC);
        _collect(v1, e);
        _collect(v2, e);
        assertEq(vault.accumulatedFees(), 800 * USDC);
        _post(bob, 900 * USDC);
        bytes32 lot2 = bytes32(uint256(0x200));
        _vote(v1, lot2, bob, AMT, dl);
        _vote(v2, lot2, bob, AMT, dl);
        assertEq(vault.accumulatedFees(), 300 * USDC);
    }
}

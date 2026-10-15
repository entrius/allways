// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {AllwaysUsdcVault} from "../src/AllwaysUsdcVault.sol";
import {VaultTestBase} from "./VaultTestBase.sol";

/// A locked miner may unlock its own bond after DEAD_MAN_DELAY of validator silence.
contract DeadManTest is VaultTestBase {
    uint256 internal constant DELAY = 14 days;

    function setUp() public override {
        super.setUp();
        vault = _deploy(_set3(v1, v2, v3), 100);
        _post(bob, 1000 * USDC);
        _lock(bob);
    }

    function _selfUnlockAvailableAt() internal view returns (uint64) {
        return vault.lastValidatorActivity() + uint64(DELAY) + 1;
    }

    function test_SelfUnlockRefusedWhileValidatorsActive() public {
        skip(DELAY);
        bytes memory err =
            abi.encodeWithSelector(AllwaysUsdcVault.DeadManNotElapsed.selector, _selfUnlockAvailableAt());
        vm.prank(bob);
        vm.expectRevert(err);
        vault.selfUnlock();

        // One ping a day keeps it shut indefinitely.
        for (uint256 d; d < 30; ++d) {
            skip(1 days);
            vm.prank(v1);
            vault.ping();
        }
        skip(DELAY);
        err = abi.encodeWithSelector(AllwaysUsdcVault.DeadManNotElapsed.selector, _selfUnlockAvailableAt());
        vm.prank(bob);
        vm.expectRevert(err);
        vault.selfUnlock();
    }

    function test_SelfUnlockAfterDelay() public {
        skip(DELAY + 1);
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.BondLockChanged(bob, false, 2, _pk(bob));
        vm.prank(bob);
        vault.selfUnlock();
        (bool locked,) = _locked(bob);
        assertFalse(locked);
        vm.prank(bob);
        vault.withdrawCollateral(1000 * USDC);
        assertEq(usdc.balanceOf(bob), 1000 * USDC);

        // Only a locked bond can self-unlock.
        vm.prank(bob);
        vm.expectRevert(AllwaysUsdcVault.LockStateUnchanged.selector);
        vault.selfUnlock();
        vm.prank(carol);
        vm.expectRevert(AllwaysUsdcVault.LockStateUnchanged.selector);
        vault.selfUnlock();
    }

    /// Each successful validator entry point, then acceptValidator, restarts the clock.
    function test_AnyValidatorTxResetsTheClock() public {
        _post(carol, 1000 * USDC);
        _lock(carol);
        usdc.mint(address(vault), 100 * USDC);
        AllwaysUsdcVault.Config memory cfg = _cfg(66);

        for (uint256 i; i < 9; ++i) {
            skip(DELAY);
            uint256 t = block.timestamp;
            vm.startPrank(v1);
            if (i == 0) vault.ping();
            if (i == 1) vault.voteUnlock(carol, 1);
            if (i == 2) vault.voteSlash(carol, bytes32(uint256(1)), 1, user, 0);
            if (i == 3) vault.voteCollectFeesBatch(_entries1(carol, 1));
            if (i == 4) vault.votePayLot(bytes32(uint256(1)), carol, 1 * USDC, uint64(t + 60));
            if (i == 5) vault.voteAddValidator(frank);
            if (i == 6) vault.voteRemoveValidator(v3);
            if (i == 7) vault.voteSetConfig(cfg);
            vm.stopPrank();
            if (i == 8) {
                vm.prank(v1);
                vault.voteAddValidator(v4);
                vm.prank(v2);
                vault.voteAddValidator(v4);
                vm.prank(v3);
                vault.voteAddValidator(v4);
                skip(10);
                vm.prank(v4);
                vault.acceptValidator();
            }
            assertEq(vault.lastValidatorActivity(), block.timestamp, "clock not reset");
            vm.prank(bob);
            vm.expectRevert(
                abi.encodeWithSelector(
                    AllwaysUsdcVault.DeadManNotElapsed.selector, block.timestamp + DELAY + 1
                )
            );
            vault.selfUnlock();
        }
    }

    function test_RevertedVoteDoesNotReset() public {
        uint64 before = vault.lastValidatorActivity();
        skip(DELAY + 1);
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.EpochMismatch.selector);
        vault.voteUnlock(bob, 7);
        assertEq(vault.lastValidatorActivity(), before);
        // A validator acting as a miner is not validator activity either.
        _post(v1, 10 * USDC);
        assertEq(vault.lastValidatorActivity(), before);
        vm.prank(bob);
        vault.selfUnlock();
    }

    function test_SelfUnlockMovesNoFunds() public {
        usdc.blacklist(user);
        bytes32 ref = bytes32(uint256(9));
        _slash(v1, bob, ref, 100 * USDC, user, 100 * USDC);
        _slash(v2, bob, ref, 100 * USDC, user, 100 * USDC);
        _slash(v3, bob, ref, 100 * USDC, user, 100 * USDC);
        uint256 bal = usdc.balanceOf(address(vault));
        uint256 tc = vault.totalCollateral();
        uint256 ps = vault.pendingSlashTotal();
        skip(DELAY + 1);
        vm.prank(bob);
        vault.selfUnlock();
        assertEq(usdc.balanceOf(address(vault)), bal);
        assertEq(vault.totalCollateral(), tc);
        assertEq(vault.pendingSlashTotal(), ps, "parked slash untouched");
        assertEq(_collateral(bob), 900 * USDC);
    }

    function test_SelfUnlockBumpsEpoch() public {
        skip(DELAY + 1);
        vm.prank(bob);
        vault.selfUnlock();
        (bool locked, uint64 epoch) = _locked(bob);
        assertFalse(locked);
        assertEq(epoch, 2);
        // A returning validator's unlock vote at the old epoch no longer applies.
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.LockStateUnchanged.selector);
        vault.voteUnlock(bob, 1);
        _lock(bob);
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.EpochMismatch.selector);
        vault.voteUnlock(bob, 1);
        (, epoch) = _locked(bob);
        assertEq(epoch, 3);
    }
}

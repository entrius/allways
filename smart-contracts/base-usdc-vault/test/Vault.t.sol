// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {AllwaysUsdcVault} from "../src/AllwaysUsdcVault.sol";
import {EighteenDecimalToken} from "./mocks/MockTokens.sol";
import {VaultTestBase} from "./VaultTestBase.sol";

/// Bond lifecycle, slash, fee settle and governance: the TAO vault's suite, ported.
contract VaultTest is VaultTestBase {
    // ── bonds and locks ─────────────────────────────────────────────────────

    function test_LockBlocksWithdrawUntilQuorumUnlock() public {
        vault = _deploy(_set3(v1, v2, v3), 100);
        _post(bob, 5000 * USDC);
        _lock(bob);
        (bool locked, uint64 epoch) = _locked(bob);
        assertTrue(locked);
        assertEq(epoch, 1);
        vm.prank(bob);
        vm.expectRevert(AllwaysUsdcVault.BondLocked.selector);
        vault.withdrawCollateral(1000 * USDC);

        vm.prank(v1);
        vault.voteUnlock(bob, 1);
        vm.prank(v2);
        vault.voteUnlock(bob, 1);
        (locked,) = _locked(bob);
        assertTrue(locked, "below quorum stays locked");

        vm.prank(v3);
        vault.voteUnlock(bob, 1);
        (locked, epoch) = _locked(bob);
        assertFalse(locked);
        assertEq(epoch, 2);

        vm.prank(bob);
        vault.withdrawCollateral(1000 * USDC);
        assertEq(_collateral(bob), 4000 * USDC);
        assertEq(usdc.balanceOf(bob), 1000 * USDC);
    }

    function test_UnlockVoteRequiresCurrentEpoch() public {
        vault = _deploy(_set3(v1, v2, v3), 100);
        _post(bob, 5000 * USDC);
        _lock(bob);
        vm.startPrank(v1);
        vm.expectRevert(AllwaysUsdcVault.EpochMismatch.selector);
        vault.voteUnlock(bob, 0);
        vm.expectRevert(AllwaysUsdcVault.EpochMismatch.selector);
        vault.voteUnlock(bob, 2);
        // An unlocked bond has no unlock to vote on.
        vm.expectRevert(AllwaysUsdcVault.LockStateUnchanged.selector);
        vault.voteUnlock(carol, 0);
        vm.stopPrank();
    }

    function test_LockRequiresMinCollateral() public {
        _post(bob, MIN_COLLATERAL - 1);
        vm.prank(bob);
        vm.expectRevert(AllwaysUsdcVault.InsufficientCollateral.selector);
        vault.lockBond(_pk(bob));
        _post(bob, 1);
        _lock(bob);
        vm.prank(bob);
        vm.expectRevert(AllwaysUsdcVault.LockStateUnchanged.selector);
        vault.lockBond(_pk(bob));
    }

    function test_LoneValidatorCanLockAndUnlock() public {
        vault = _deploy(_set1(v1), 100);
        _post(bob, 5000 * USDC);
        _lock(bob);
        vm.prank(v1);
        vault.voteUnlock(bob, 1);
        (bool locked, uint64 epoch) = _locked(bob);
        assertFalse(locked);
        assertEq(epoch, 2);
    }

    function test_LockSucceedsBelowRemovalFloor() public {
        _post(bob, 5000 * USDC);
        _lock(bob);
        (bool locked, uint64 epoch) = _locked(bob);
        assertTrue(locked);
        assertEq(epoch, 1);
    }

    // ── slash ───────────────────────────────────────────────────────────────

    function test_SlashReimbursesUserAndCreditsSurplus() public {
        _post(bob, 5000 * USDC);
        bytes32 ref = bytes32(uint256(7));
        _slash(v1, bob, ref, 3000 * USDC, carol, 2500 * USDC);
        assertEq(_collateral(bob), 5000 * USDC, "not yet quorum");
        assertFalse(vault.isSlashed(ref));

        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.MinerSlashed(bob, ref, 3000 * USDC, 2500 * USDC, 500 * USDC);
        _slash(v2, bob, ref, 3000 * USDC, carol, 2500 * USDC);
        assertTrue(vault.isSlashed(ref));
        assertEq(_collateral(bob), 2000 * USDC);
        assertEq(vault.accumulatedFees(), 500 * USDC);
        assertEq(usdc.balanceOf(carol), 2500 * USDC, "pushed directly");
        assertEq(vault.pendingSlashTotal(), 0);
        assertEq(vault.pot(), 500 * USDC);

        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.AlreadySlashed.selector);
        vault.voteSlash(bob, ref, 3000 * USDC, carol, 2500 * USDC);
    }

    function test_DivergentSlashVerdictsOpenSeparateRounds() public {
        vault = _deploy(_set3(v1, v2, v3), 66);
        _post(bob, 5000 * USDC);
        bytes32 ref = bytes32(uint256(9));
        _slash(v3, bob, ref, 5000 * USDC, v3, 5000 * USDC);
        _slash(v1, bob, ref, 3000 * USDC, frank, 2500 * USDC);
        _slash(v2, bob, ref, 3000 * USDC, frank, 2500 * USDC);
        assertTrue(vault.isSlashed(ref));
        assertEq(_collateral(bob), 2000 * USDC);
        assertEq(vault.accumulatedFees(), 500 * USDC);

        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.AlreadySlashed.selector);
        vault.voteSlash(bob, ref, 5000 * USDC, v3, 5000 * USDC);
    }

    function test_SlashRoundHashBindsAllArgs() public {
        _post(bob, 5000 * USDC);
        bytes32 ref = bytes32(uint256(9));
        _slash(v1, bob, ref, 3000 * USDC, carol, 2500 * USDC);
        // Each differing field opens its own round; none reaches quorum.
        _slash(v2, carol, ref, 3000 * USDC, carol, 2500 * USDC);
        _slash(v2, bob, bytes32(uint256(10)), 3000 * USDC, carol, 2500 * USDC);
        _slash(v2, bob, ref, 2000 * USDC, carol, 1500 * USDC);
        _slash(v2, bob, ref, 3000 * USDC, frank, 2500 * USDC);
        _slash(v2, bob, ref, 3000 * USDC, carol, 2400 * USDC);
        assertFalse(vault.isSlashed(ref));
        assertEq(_collateral(bob), 5000 * USDC);

        _slash(v2, bob, ref, 3000 * USDC, carol, 2500 * USDC);
        assertTrue(vault.isSlashed(ref));
        assertEq(_collateral(bob), 2000 * USDC);
    }

    function test_SeizeClampsToCollateral() public {
        _post(bob, 1500 * USDC);
        bytes32 ref = bytes32(uint256(3));
        _slash(v1, bob, ref, 3000 * USDC, carol, 3000 * USDC);
        _slash(v2, bob, ref, 3000 * USDC, carol, 3000 * USDC);
        assertEq(_collateral(bob), 0);
        assertEq(vault.accumulatedFees(), 0);
        assertEq(usdc.balanceOf(carol), 1500 * USDC);

        // A miner with no bond: nothing seized, but the marker is still set.
        bytes32 ref2 = bytes32(uint256(4));
        _slash(v1, frank, ref2, 100 * USDC, carol, 0);
        _slash(v2, frank, ref2, 100 * USDC, carol, 0);
        assertTrue(vault.isSlashed(ref2));
    }

    function test_SlashArgumentChecks() public {
        vm.startPrank(v1);
        vm.expectRevert(AllwaysUsdcVault.InvalidAmount.selector);
        vault.voteSlash(bob, bytes32(uint256(1)), 0, carol, 0);
        vm.expectRevert(AllwaysUsdcVault.InvalidReimbursement.selector);
        vault.voteSlash(bob, bytes32(uint256(1)), 1, carol, 2);
        vm.stopPrank();
    }

    // ── fee settle ──────────────────────────────────────────────────────────

    function test_BatchSettleAppliesDeltasAndReplayNoops() public {
        _post(bob, 5000 * USDC);
        _post(carol, 4000 * USDC);
        AllwaysUsdcVault.FeeEntry[] memory batch = _entries2(bob, 300 * USDC, carol, 200 * USDC);
        _collect(v1, batch);
        _collect(v2, batch);
        assertEq(_collateral(bob), 4700 * USDC);
        assertEq(_collateral(carol), 3800 * USDC);
        assertEq(vault.settledTotal(bob), 300 * USDC);
        assertEq(vault.accumulatedFees(), 500 * USDC);

        _collect(v1, batch);
        _collect(v2, batch);
        assertEq(_collateral(bob), 4700 * USDC);
        assertEq(vault.accumulatedFees(), 500 * USDC);

        AllwaysUsdcVault.FeeEntry[] memory next = _entries1(bob, 450 * USDC);
        _collect(v1, next);
        _collect(v2, next);
        assertEq(_collateral(bob), 4550 * USDC);
        assertEq(vault.settledTotal(bob), 450 * USDC);
        assertEq(vault.accumulatedFees(), 650 * USDC);
        assertEq(vault.pot(), 650 * USDC);
    }

    function test_BatchSettleClampsToCollateralAndReportsShortfall() public {
        _post(bob, 1000 * USDC);
        AllwaysUsdcVault.FeeEntry[] memory batch = _entries1(bob, 1500 * USDC);
        _collect(v1, batch);
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.FeesSettled(bob, 1000 * USDC, 1500 * USDC, 500 * USDC);
        _collect(v2, batch);
        assertEq(_collateral(bob), 0);
        assertEq(vault.accumulatedFees(), 1000 * USDC);
        assertEq(vault.settledTotal(bob), 1500 * USDC);
    }

    function test_BatchRoundsAreKeyedByContentsHash() public {
        _post(bob, 5000 * USDC);
        _post(carol, 4000 * USDC);
        AllwaysUsdcVault.FeeEntry[] memory cadence = _entries2(bob, 300 * USDC, carol, 200 * USDC);
        AllwaysUsdcVault.FeeEntry[] memory exit = _entries1(bob, 350 * USDC);
        _collect(v1, cadence);
        _collect(v1, exit);
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.AlreadyVoted.selector);
        vault.voteCollectFeesBatch(cadence);

        _collect(v2, exit);
        assertEq(vault.settledTotal(bob), 350 * USDC);
        _collect(v2, cadence);
        assertEq(vault.settledTotal(bob), 350 * USDC);
        assertEq(_collateral(bob), 4650 * USDC);
        assertEq(vault.settledTotal(carol), 200 * USDC);
        assertEq(vault.accumulatedFees(), 550 * USDC);
    }

    function test_BatchSettleValidatesBatchSize() public {
        _post(bob, 5000 * USDC);
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.InvalidBatch.selector);
        vault.voteCollectFeesBatch(new AllwaysUsdcVault.FeeEntry[](0));

        AllwaysUsdcVault.FeeEntry[] memory oversize = new AllwaysUsdcVault.FeeEntry[](257);
        for (uint256 i; i < 257; ++i) {
            oversize[i] = AllwaysUsdcVault.FeeEntry(address(uint160(i + 1)), 1);
        }
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.InvalidBatch.selector);
        vault.voteCollectFeesBatch(oversize);

        // Exactly MAX_BATCH is accepted.
        AllwaysUsdcVault.FeeEntry[] memory full = new AllwaysUsdcVault.FeeEntry[](256);
        for (uint256 i; i < 256; ++i) {
            full[i] = AllwaysUsdcVault.FeeEntry(address(uint160(i + 1)), 1);
        }
        _collect(v1, full);

        AllwaysUsdcVault.FeeEntry[] memory batch = _entries1(bob, 100 * USDC);
        _collect(v1, batch);
        _collect(v2, batch);
        assertEq(vault.settledTotal(bob), 100 * USDC);
    }

    // ── governance ──────────────────────────────────────────────────────────

    function test_ConstructorRefusesASetThatWouldBrickTheVault() public {
        IERC20 t = IERC20(address(usdc));
        AllwaysUsdcVault.Config memory cfg = _cfg(100);

        vm.expectRevert(AllwaysUsdcVault.InvalidValidatorSet.selector);
        new AllwaysUsdcVault(t, new address[](0), cfg);

        address[] memory dupes = _set3(v1, v2, v1);
        vm.expectRevert(AllwaysUsdcVault.InvalidValidatorSet.selector);
        new AllwaysUsdcVault(t, dupes, cfg);

        vm.expectRevert(AllwaysUsdcVault.InvalidValidatorSet.selector);
        new AllwaysUsdcVault(t, _set2(v1, address(0)), cfg);

        address[] memory seventeen = new address[](17);
        for (uint256 i; i < 17; ++i) {
            seventeen[i] = address(uint160(0x1000 + i));
        }
        vm.expectRevert(AllwaysUsdcVault.InvalidValidatorSet.selector);
        new AllwaysUsdcVault(t, seventeen, cfg);

        // The vault's own address, predicted from the deployer nonce.
        address predicted = vm.computeCreateAddress(address(this), vm.getNonce(address(this)));
        vm.expectRevert(AllwaysUsdcVault.InvalidValidatorSet.selector);
        new AllwaysUsdcVault(t, _set2(v1, predicted), cfg);

        cfg.consensusThresholdPct = 50;
        vm.expectRevert(abi.encodeWithSelector(AllwaysUsdcVault.ConfigOutOfBounds.selector, 2));
        new AllwaysUsdcVault(t, _set1(v1), cfg);
        cfg.consensusThresholdPct = 100;
        cfg.voteRoundTtl = 599;
        vm.expectRevert(abi.encodeWithSelector(AllwaysUsdcVault.ConfigOutOfBounds.selector, 3));
        new AllwaysUsdcVault(t, _set1(v1), cfg);
        cfg.voteRoundTtl = 600;

        // The token must be a contract with 6 decimals.
        vm.expectRevert(AllwaysUsdcVault.InvalidToken.selector);
        new AllwaysUsdcVault(IERC20(makeAddr("eoa")), _set1(v1), cfg);
        EighteenDecimalToken weth = new EighteenDecimalToken();
        vm.expectRevert(AllwaysUsdcVault.InvalidToken.selector);
        new AllwaysUsdcVault(IERC20(address(weth)), _set1(v1), cfg);

        address[] memory sixteen = new address[](16);
        for (uint256 i; i < 16; ++i) {
            sixteen[i] = address(uint160(0x1000 + i));
        }
        AllwaysUsdcVault ok = new AllwaysUsdcVault(t, sixteen, cfg);
        assertEq(ok.getValidators().length, 16);
        assertEq(ok.VERSION(), 1);
        assertEq(address(ok.usdc()), address(usdc));
        assertEq(ok.pot(), 0);
        assertEq(ok.lastValidatorActivity(), block.timestamp);
    }

    function test_ThresholdFloorIsStrictMajorityAtEverySetSize() public {
        for (uint256 n = 1; n <= 16; ++n) {
            address[] memory set = new address[](n);
            for (uint256 i; i < n; ++i) {
                set[i] = address(uint160(0x1000 + i));
            }
            AllwaysUsdcVault vlt = _deploy(set, 51);
            assertGt(vlt.requiredVotes() * 2, n);
        }
    }

    function test_RampFromOneValidatorNeedsUnanimityAtEveryStep() public {
        vault = _deploy(_set1(v1), 100);
        vm.prank(v1);
        vault.voteAddValidator(v2);
        assertEq(vault.getValidators(), _set1(v1), "approved, not yet joined");
        assertEq(vault.getPendingValidators(), _set1(v2));
        vm.prank(v2);
        vault.acceptValidator();
        assertEq(vault.getValidators(), _set2(v1, v2));

        vm.prank(v1);
        vault.voteAddValidator(v3);
        assertEq(vault.getPendingValidators().length, 0);
        vm.prank(v2);
        vault.voteAddValidator(v3);
        assertEq(vault.getPendingValidators(), _set1(v3));
    }

    function test_AcceptIsRequiredAndOnlyByTheCandidate() public {
        vault = _deploy(_set1(v1), 100);
        vm.prank(v1);
        vault.voteAddValidator(v2);

        vm.prank(carol);
        vm.expectRevert(AllwaysUsdcVault.NotPendingValidator.selector);
        vault.acceptValidator();
        vm.startPrank(v2);
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.voteAddValidator(v3);
        vault.acceptValidator();
        vm.expectRevert(AllwaysUsdcVault.NotPendingValidator.selector);
        vault.acceptValidator();
        vm.stopPrank();
        assertTrue(vault.isValidator(v2));
    }

    function test_ExpiredAdmissionCanNeverAccept() public {
        vault = _deploy(_set1(v1), 100);
        vm.prank(v1);
        vault.voteAddValidator(v2);
        assertEq(vault.getPendingValidators(), _set1(v2));

        skip(uint256(vault.voteRoundTtl()) + 1);
        assertEq(vault.getPendingValidators().length, 0);
        vm.prank(v2);
        vm.expectRevert(AllwaysUsdcVault.AdmissionVoid.selector);
        vault.acceptValidator();
        assertEq(vault.getValidators(), _set1(v1));
    }

    function test_SetChangeVoidsPendingAdmission() public {
        vault = _deploy(_set3(v1, v2, v3), 100);
        vm.prank(v1);
        vault.voteAddValidator(frank);
        vm.prank(v2);
        vault.voteAddValidator(frank);
        vm.prank(v3);
        vault.voteAddValidator(frank);
        assertEq(vault.getPendingValidators(), _set1(frank));

        vm.prank(v1);
        vault.voteRemoveValidator(v3);
        vm.prank(v2);
        vault.voteRemoveValidator(v3);

        assertEq(vault.getPendingValidators().length, 0);
        vm.prank(frank);
        vm.expectRevert(AllwaysUsdcVault.AdmissionVoid.selector);
        vault.acceptValidator();
        assertEq(vault.getValidators(), _set2(v1, v2));
    }

    function test_ExpiredAdmissionsAreSweptAndSlotsReclaimed() public {
        vault = _deploy(_set1(v1), 100);
        vm.startPrank(v1);
        for (uint160 i; i < 15; ++i) {
            vault.voteAddValidator(address(0x100 + i));
        }
        vm.expectRevert(AllwaysUsdcVault.InvalidValidatorSet.selector);
        vault.voteAddValidator(v2);

        // One accepts in time; that set change voids the other fourteen.
        vm.stopPrank();
        vm.prank(address(0x100));
        vault.acceptValidator();
        assertEq(vault.getPendingValidators().length, 0);

        skip(uint256(vault.voteRoundTtl()) + 1);
        // Every slot is reclaimed: fourteen fresh admissions fit again (2 validators + 14 pending).
        vm.prank(v1);
        vault.voteAddValidator(v2);
        vm.prank(address(0x100));
        vault.voteAddValidator(v2);
        for (uint160 i = 1; i < 14; ++i) {
            vm.prank(v1);
            vault.voteAddValidator(address(0x200 + i));
            vm.prank(address(0x100));
            vault.voteAddValidator(address(0x200 + i));
        }
        assertEq(vault.getPendingValidators().length, 14);
    }

    function test_SweepKeepsLiveAdmissionsInOrder() public {
        vault = _deploy(_set1(v1), 100);
        vm.startPrank(v1);
        vault.voteAddValidator(address(0x100));
        skip(1000);
        vault.voteAddValidator(address(0x101));
        vault.voteAddValidator(address(0x102));
        skip(900);
        // 0x100 is now dead; the sweep compacts the two live ones down, in order.
        vault.voteAddValidator(address(0x103));
        vm.stopPrank();
        address[] memory pend = vault.getPendingValidators();
        assertEq(pend.length, 3);
        assertEq(pend[0], address(0x101));
        assertEq(pend[1], address(0x102));
        assertEq(pend[2], address(0x103));
        // Accepting the middle one removes it from the list.
        vm.prank(address(0x102));
        vault.acceptValidator();
        assertEq(vault.getValidators().length, 2);
    }

    function test_RemovalExcludesTargetAndHoldsTheFloor() public {
        vault = _deploy(_set3(v1, v2, v3), 100);
        vm.prank(v3);
        vm.expectRevert(AllwaysUsdcVault.SelfRemoval.selector);
        vault.voteRemoveValidator(v3);
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.voteRemoveValidator(frank);

        vm.prank(v1);
        vault.voteRemoveValidator(v2);
        assertEq(vault.getValidators().length, 3);
        vm.prank(v3);
        vault.voteRemoveValidator(v2);
        assertEq(vault.getValidators(), _set2(v1, v3), "order preserved");
        assertFalse(vault.isValidator(v2));

        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.InvalidValidatorSet.selector);
        vault.voteRemoveValidator(v3);
    }

    function test_MembershipRoundsBoundToTheirSet() public {
        vault = _deploy(_set3(v1, v2, v3), 100);
        vm.prank(v1);
        vault.voteAddValidator(frank);

        vm.prank(v1);
        vault.voteRemoveValidator(v3);
        vm.prank(v2);
        vault.voteRemoveValidator(v3);

        vm.prank(v2);
        vm.expectRevert(AllwaysUsdcVault.PendingConflict.selector);
        vault.voteAddValidator(frank);
    }

    function test_RemovedValidatorsVotesStopCounting() public {
        // At threshold 66, n=3 needs 2.
        vault = _deploy(_set3(v1, v2, v3), 66);
        _post(bob, 5000 * USDC);
        bytes32 ref = bytes32(uint256(21));
        _slash(v3, bob, ref, 1000 * USDC, frank, 0);

        vm.prank(v1);
        vault.voteRemoveValidator(v3);
        vm.prank(v2);
        vault.voteRemoveValidator(v3);

        // n=2 at 66% needs 2; v3's stale vote no longer counts.
        _slash(v1, bob, ref, 1000 * USDC, frank, 0);
        assertFalse(vault.isSlashed(ref));
        assertEq(_collateral(bob), 5000 * USDC);
    }

    function test_ConfigRoundIsUnanimousAndValidated() public {
        AllwaysUsdcVault.Config memory cfg = _cfg(66);
        cfg.minCollateral = 500e6;
        cfg.maxCollateral = 9000e6;
        cfg.voteRoundTtl = 600;

        AllwaysUsdcVault.Config memory bad = _clone(cfg);
        bad.consensusThresholdPct = 101;
        vm.prank(v1);
        vm.expectRevert(abi.encodeWithSelector(AllwaysUsdcVault.ConfigOutOfBounds.selector, 2));
        vault.voteSetConfig(bad);
        bad.consensusThresholdPct = 50;
        vm.prank(v1);
        vm.expectRevert(abi.encodeWithSelector(AllwaysUsdcVault.ConfigOutOfBounds.selector, 2));
        vault.voteSetConfig(bad);

        vm.prank(v1);
        vault.voteSetConfig(cfg);
        assertEq(vault.minCollateral(), MIN_COLLATERAL);
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.ConfigUpdated(cfg);
        vm.prank(v2);
        vault.voteSetConfig(cfg);
        assertEq(vault.minCollateral(), 500e6);
        assertEq(vault.maxCollateral(), 9000e6);
        assertEq(vault.consensusThresholdPct(), 66);
        assertEq(vault.voteRoundTtl(), 600);
        assertEq(vault.maxLotAmount(), 1000e6);
    }

    function test_ConfigRoundsAgreeOnWholeTuple() public {
        AllwaysUsdcVault.Config memory a = _cfg(66);
        a.minCollateral = 500e6;
        AllwaysUsdcVault.Config memory b = _clone(a);
        b.consensusThresholdPct = 51;
        vm.prank(v1);
        vault.voteSetConfig(a);
        vm.prank(v2);
        vault.voteSetConfig(b);
        assertEq(vault.minCollateral(), MIN_COLLATERAL);
        assertEq(vault.consensusThresholdPct(), 100);

        vm.prank(v2);
        vault.voteSetConfig(a);
        assertEq(vault.minCollateral(), 500e6);
        assertEq(vault.consensusThresholdPct(), 66);
    }

    function test_NonValidatorsCannotTouchGovernance() public {
        vm.startPrank(deployer);
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.voteAddValidator(frank);
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.voteRemoveValidator(v2);
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.voteSetConfig(_cfg(66));
        vm.stopPrank();
    }

    function test_DuplicateAdmissionRefused() public {
        vault = _deploy(_set1(v1), 100);
        vm.startPrank(v1);
        vm.expectRevert(AllwaysUsdcVault.AlreadyValidator.selector);
        vault.voteAddValidator(v1);
        vault.voteAddValidator(v2);
        vm.expectRevert(AllwaysUsdcVault.AlreadyValidator.selector);
        vault.voteAddValidator(v2);
        vm.expectRevert(AllwaysUsdcVault.ZeroAddress.selector);
        vault.voteAddValidator(address(0));
        vm.expectRevert(AllwaysUsdcVault.InvalidValidatorSet.selector);
        vault.voteAddValidator(address(vault));
        vm.stopPrank();
    }

    function test_TotalCollateralTracksMappingThroughEveryPath() public {
        _post(bob, 5000 * USDC);
        _post(carol, 4000 * USDC);
        _post(bob, 1000 * USDC);

        bytes32 ref = bytes32(uint256(13));
        _slash(v1, bob, ref, 2000 * USDC, v2, 1500 * USDC);
        _slash(v2, bob, ref, 2000 * USDC, v2, 1500 * USDC);
        AllwaysUsdcVault.FeeEntry[] memory batch = _entries1(carol, 400 * USDC);
        _collect(v1, batch);
        _collect(v2, batch);
        vm.prank(carol);
        vault.withdrawCollateral(600 * USDC);

        assertEq(vault.totalCollateral(), _collateral(bob) + _collateral(carol));
        assertEq(vault.totalCollateral(), 4000 * USDC + 3000 * USDC);
        assertGe(
            usdc.balanceOf(address(vault)),
            vault.totalCollateral() + vault.accumulatedFees() + vault.pendingSlashTotal()
        );
    }

    // ── round plumbing and views ────────────────────────────────────────────

    function test_ExpiredRoundIsReplacedAndViewsTrackIt() public {
        _post(bob, 5000 * USDC);
        bytes32 ref = bytes32(uint256(5));
        _slash(v1, bob, ref, 100 * USDC, carol, 0);
        bytes32 key = vault.roundKeyForSlash(bob, ref, 100 * USDC, carol, 0);
        assertTrue(vault.hasVoted(key, v1));
        assertFalse(vault.hasVoted(key, v2));
        (bytes32 h, uint64 id, uint64 at, address[] memory voters) = vault.getRound(key);
        assertEq(h, key);
        assertEq(id, 1);
        assertEq(at, block.timestamp);
        assertEq(voters, _set1(v1));
        assertFalse(vault.hasVoted(bytes32(0), v1));

        skip(1801);
        assertFalse(vault.hasVoted(key, v1), "expired round reads as not voted");
        // The expired round is replaced: v2 alone opens round 2 instead of completing round 1.
        _slash(v2, bob, ref, 100 * USDC, carol, 0);
        (, id,,) = vault.getRound(key);
        assertEq(id, 2);
        assertFalse(vault.isSlashed(ref));
        assertEq(vault.nextRoundId(), 3);
    }

    function test_RoundKeyHelpersMatchVotes() public {
        vault = _deploy(_set3(v1, v2, v3), 100);
        _post(bob, 5000 * USDC);
        _lock(bob);

        vm.prank(v1);
        vault.voteUnlock(bob, 1);
        assertTrue(vault.hasVoted(vault.roundKeyForUnlock(bob), v1));

        AllwaysUsdcVault.FeeEntry[] memory batch = _entries1(bob, 1);
        _collect(v1, batch);
        assertTrue(vault.hasVoted(vault.roundKeyForCollect(batch), v1));

        vm.prank(v1);
        vault.voteAddValidator(frank);
        assertTrue(vault.hasVoted(vault.roundKeyForAdd(frank), v1));

        vm.prank(v1);
        vault.voteRemoveValidator(v3);
        assertTrue(vault.hasVoted(vault.roundKeyForRemove(v3), v1));

        AllwaysUsdcVault.Config memory cfg = _cfg(66);
        vm.prank(v1);
        vault.voteSetConfig(cfg);
        assertTrue(vault.hasVoted(vault.roundKeyForConfig(cfg, vault.setHash()), v1));

        assertEq(vault.setHash(), keccak256(abi.encode(_set3(v1, v2, v3))));
        AllwaysUsdcVault.Config memory got = vault.getConfig();
        assertEq(got.voteRoundTtl, 1800);
        (uint256 c, bool locked, uint64 epoch, bytes32 pk) = vault.getBond(bob);
        assertEq(c, 5000 * USDC);
        assertTrue(locked);
        assertEq(epoch, 1);
        assertEq(pk, _pk(bob));
    }

    function test_VoteCastEventCarriesSubjectAndCount() public {
        _post(bob, 5000 * USDC);
        _lock(bob);
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.VaultVoteCast(v1, 0, 1, bytes32(uint256(uint160(bob))), 1);
        vm.prank(v1);
        vault.voteUnlock(bob, 1);
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.BondLockChanged(bob, false, 2, _pk(bob));
        vm.prank(v2);
        vault.voteUnlock(bob, 1);
    }
}

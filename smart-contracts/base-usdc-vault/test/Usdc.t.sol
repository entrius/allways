// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {AllwaysUsdcVault} from "../src/AllwaysUsdcVault.sol";
import {
    FeeOnTransferToken,
    GasBurnerToken,
    ReturnBombToken,
    OddReturnToken,
    ReentrantToken,
    SixDecimalToken
} from "./mocks/MockTokens.sol";
import {VaultTestBase} from "./VaultTestBase.sol";

/// Behaviour specific to an ERC-20 bond on an EVM chain: pot accounting, deposits, push/pull
/// transfers, issuer blacklist and pause, and hostile tokens.
contract UsdcTest is VaultTestBase {
    bytes32 internal constant REF = bytes32(uint256(0xabc));

    function _quorumSlash(address m, address payee, uint256 penalty, uint256 reimb) internal {
        _slash(v1, m, REF, penalty, payee, reimb);
        _slash(v2, m, REF, penalty, payee, reimb);
    }

    // ── pot ─────────────────────────────────────────────────────────────────

    function test_PlainTransferToVaultIsPot() public {
        _post(bob, 5000 * USDC);
        assertEq(vault.pot(), 0);
        usdc.mint(carol, 2000 * USDC);
        vm.prank(carol);
        usdc.transfer(address(vault), 2000 * USDC);
        assertEq(vault.accumulatedFees(), 0, "ledger knows nothing about it");
        assertEq(vault.pot(), 2000 * USDC);
        assertEq(vault.totalCollateral(), 5000 * USDC);
    }

    function test_PotNeverReachesCollateralOrPendingClaims() public {
        _post(bob, 5000 * USDC);
        _post(carol, 4000 * USDC);
        assertEq(vault.pot(), 0);

        // A fully reimbursed slash leaves no surplus, pushed or parked.
        usdc.blacklist(frank);
        _quorumSlash(bob, frank, 1000 * USDC, 1000 * USDC);
        assertEq(vault.pendingSlashTotal(), 1000 * USDC);
        assertEq(vault.totalCollateral(), 8000 * USDC);
        assertEq(vault.pot(), 0);

        // Even if the issuer removes USDC from the vault, the pot saturates at zero.
        usdc.wipe(address(vault), 500 * USDC);
        assertEq(vault.pot(), 0);
    }

    // ── deposits ────────────────────────────────────────────────────────────

    function test_PostCreditsMeasuredDeltaAndRejectsFeeOnTransfer() public {
        FeeOnTransferToken fot = new FeeOnTransferToken();
        AllwaysUsdcVault fv = new AllwaysUsdcVault(IERC20(address(fot)), _set1(v1), _cfg(100));
        fot.mint(bob, 1000 * USDC);
        vm.startPrank(bob);
        fot.approve(address(fv), 1000 * USDC);
        vm.expectRevert(
            abi.encodeWithSelector(AllwaysUsdcVault.DepositMismatch.selector, 1000 * USDC, 990 * USDC)
        );
        fv.postCollateral(1000 * USDC);
        vm.stopPrank();
        assertEq(fv.totalCollateral(), 0);

        // The honest token credits exactly what arrived.
        _post(bob, 1000 * USDC);
        assertEq(_collateral(bob), 1000 * USDC);
        assertEq(usdc.balanceOf(address(vault)), 1000 * USDC);
    }

    function test_PostArgumentChecks() public {
        vm.startPrank(bob);
        vm.expectRevert(AllwaysUsdcVault.InvalidAmount.selector);
        vault.postCollateral(0);
        vm.expectRevert(AllwaysUsdcVault.ExceedsMaxCollateral.selector);
        vault.postCollateral(100_000 * USDC + 1);
        vm.stopPrank();
        _post(bob, 100_000 * USDC);
        vm.prank(bob);
        vm.expectRevert(AllwaysUsdcVault.ExceedsMaxCollateral.selector);
        vault.postCollateral(1);
    }

    function test_UnboundedMaxStillCapsAtUint128() public {
        AllwaysUsdcVault.Config memory cfg = _cfg(100);
        cfg.maxCollateral = 0;
        vault = new AllwaysUsdcVault(IERC20(address(usdc)), _set1(v1), cfg);
        uint256 big = uint256(type(uint128).max);
        _post(bob, big);
        vm.prank(bob);
        vm.expectRevert(AllwaysUsdcVault.ExceedsMaxCollateral.selector);
        vault.postCollateral(1);
    }

    function test_PermitFrontrunStillDeposits() public {
        uint256 amount = 500 * USDC;
        usdc.mint(bob, amount);
        uint256 deadline = block.timestamp + 1 hours;
        (uint8 v, bytes32 r, bytes32 s) = _signPermit(bobPk, bob, amount, deadline);

        // Someone lifts the permit from the mempool and submits it first.
        usdc.permit(bob, address(vault), amount, deadline, v, r, s);
        vm.prank(bob);
        vault.postCollateralWithPermit(amount, deadline, v, r, s);
        assertEq(_collateral(bob), amount);

        // A fresh permit works in one tx too.
        usdc.mint(bob, amount);
        (v, r, s) = _signPermit(bobPk, bob, amount, deadline);
        vm.prank(bob);
        vault.postCollateralWithPermit(amount, deadline, v, r, s);
        assertEq(_collateral(bob), 2 * amount);

        // No permit and no allowance: the pull itself refuses.
        usdc.mint(bob, amount);
        vm.prank(bob);
        vm.expectRevert();
        vault.postCollateralWithPermit(amount, deadline, 27, bytes32(0), bytes32(0));
    }

    function _signPermit(uint256 pk, address owner, uint256 value, uint256 deadline)
        internal
        view
        returns (uint8, bytes32, bytes32)
    {
        bytes32 structHash = keccak256(
            abi.encode(
                keccak256(
                    "Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)"
                ),
                owner,
                address(vault),
                value,
                usdc.nonces(owner),
                deadline
            )
        );
        return vm.sign(pk, keccak256(abi.encodePacked("\x19\x01", usdc.DOMAIN_SEPARATOR(), structHash)));
    }

    function test_WithdrawArgumentChecks() public {
        _post(bob, 100 * USDC);
        vm.startPrank(bob);
        vm.expectRevert(AllwaysUsdcVault.InvalidAmount.selector);
        vault.withdrawCollateral(0);
        vm.expectRevert(AllwaysUsdcVault.InsufficientCollateral.selector);
        vault.withdrawCollateral(100 * USDC + 1);
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.CollateralWithdrawn(bob, 100 * USDC, 0);
        vault.withdrawCollateral(100 * USDC);
        vm.stopPrank();
    }

    // ── slash push / pull ───────────────────────────────────────────────────

    function test_SlashPushToBlacklistedUserParks() public {
        _post(bob, 5000 * USDC);
        usdc.blacklist(user);
        _slash(v1, bob, REF, 3000 * USDC, user, 2500 * USDC);
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.SlashPending(REF, user, 2500 * USDC);
        _slash(v2, bob, REF, 3000 * USDC, user, 2500 * USDC);
        assertTrue(vault.isSlashed(REF));
        (address who, uint128 amt) = vault.pendingSlash(REF);
        assertEq(who, user);
        assertEq(amt, 2500 * USDC);
        assertEq(vault.pendingSlashTotal(), 2500 * USDC);
        assertEq(vault.pot(), 500 * USDC);
        _assertBooks();
    }

    function test_ClaimSlashAfterUnblacklist() public {
        _post(bob, 5000 * USDC);
        usdc.blacklist(user);
        _quorumSlash(bob, user, 3000 * USDC, 2500 * USDC);

        vm.expectRevert();
        vault.claimSlash(REF);

        usdc.unBlacklist(user);
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.SlashClaimed(REF, user, 2500 * USDC);
        vm.prank(user);
        vault.claimSlash(REF);
        assertEq(usdc.balanceOf(user), 2500 * USDC);
        assertEq(vault.pendingSlashTotal(), 0);
        vm.expectRevert(AllwaysUsdcVault.NoPendingSlash.selector);
        vault.claimSlash(REF);
    }

    function test_ClaimSlashIsPermissionlessButPaysRecordedUser() public {
        _post(bob, 5000 * USDC);
        usdc.blacklist(user);
        _quorumSlash(bob, user, 3000 * USDC, 2500 * USDC);
        usdc.unBlacklist(user);

        address keeper = makeAddr("keeper");
        vm.prank(keeper);
        vault.claimSlash(REF);
        assertEq(usdc.balanceOf(user), 2500 * USDC);
        assertEq(usdc.balanceOf(keeper), 0);
    }

    function test_ClaimSlashRevertKeepsRecord() public {
        _post(bob, 5000 * USDC);
        usdc.blacklist(user);
        _quorumSlash(bob, user, 3000 * USDC, 2500 * USDC);

        vm.expectRevert("Blacklistable: account is blacklisted");
        vault.claimSlash(REF);
        (address who, uint128 amt) = vault.pendingSlash(REF);
        assertEq(who, user);
        assertEq(amt, 2500 * USDC);
        assertEq(vault.pendingSlashTotal(), 2500 * USDC);
    }

    function test_SlashPayeeIsVaultBooksAsPot() public {
        _post(bob, 5000 * USDC);
        uint256 balBefore = usdc.balanceOf(address(vault));
        _quorumSlash(bob, address(vault), 3000 * USDC, 2500 * USDC);
        assertEq(usdc.balanceOf(address(vault)), balBefore, "no transfer");
        assertEq(vault.pendingSlashTotal(), 0);
        assertEq(vault.accumulatedFees(), 3000 * USDC);
        assertEq(vault.pot(), 3000 * USDC);
    }

    function test_SlashRejectsZeroPayee() public {
        _post(bob, 5000 * USDC);
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.InvalidPayee.selector);
        vault.voteSlash(bob, REF, 1, address(0), 0);
    }

    // ── hostile tokens ──────────────────────────────────────────────────────

    function _hostileVault(SixDecimalToken t) internal returns (AllwaysUsdcVault hv) {
        hv = new AllwaysUsdcVault(IERC20(address(t)), _set2(v1, v2), _cfg(100));
        t.mint(bob, 5000 * USDC);
        vm.startPrank(bob);
        t.approve(address(hv), 5000 * USDC);
        hv.postCollateral(5000 * USDC);
        vm.stopPrank();
        vm.prank(v1);
        hv.voteSlash(bob, REF, 3000 * USDC, user, 2500 * USDC);
    }

    function test_PushUsesFixedGasAndFallbackAlwaysWrites() public {
        GasBurnerToken t = new GasBurnerToken();
        AllwaysUsdcVault hv = _hostileVault(t);
        t.arm(true);
        uint256 g0 = gasleft();
        vm.prank(v2);
        hv.voteSlash{gas: 2_000_000}(bob, REF, 3000 * USDC, user, 2500 * USDC);
        uint256 used = g0 - gasleft();
        assertTrue(hv.isSlashed(REF));
        (address who, uint128 amt) = hv.pendingSlash(REF);
        assertEq(who, user);
        assertEq(amt, 2500 * USDC);
        // The burner ate exactly PUSH_GAS, not the 2M the voter offered.
        assertLt(used, hv.PUSH_GAS() + 250_000);
    }

    function test_PushRevertsRatherThanStarve() public {
        // A voter that can't forward the full PUSH_GAS gets a revert, never a silent park.
        _post(bob, 5000 * USDC);
        _slash(v1, bob, REF, 3000 * USDC, user, 2500 * USDC);
        bytes memory call =
            abi.encodeCall(AllwaysUsdcVault.voteSlash, (bob, REF, 3000 * USDC, user, 2500 * USDC));
        bool sawFloor;
        for (uint256 g = 150_000; g <= 350_000 && !sawFloor; g += 1000) {
            vm.prank(v2);
            (bool ok, bytes memory ret) = address(vault).call{gas: g}(call);
            if (!ok && ret.length == 4 && bytes4(ret) == AllwaysUsdcVault.InsufficientGas.selector) {
                sawFloor = true;
            }
        }
        assertTrue(sawFloor, "floor never hit");
        assertFalse(vault.isSlashed(REF));
    }

    /// The eth_estimateGas view: sweep the gas limit; at every limit the quorum vote either reverts
    /// or pays the user directly. No limit parks a payable reimbursement.
    function test_NoGasLimitSilentlyParksAPayablePush() public {
        _post(bob, 5000 * USDC);
        _slash(v1, bob, REF, 3000 * USDC, user, 2500 * USDC);
        bytes memory call =
            abi.encodeCall(AllwaysUsdcVault.voteSlash, (bob, REF, 3000 * USDC, user, 2500 * USDC));
        bool sawSuccess;
        for (uint256 g = 100_000; g <= 400_000; g += 500) {
            uint256 snap = vm.snapshotState();
            vm.prank(v2);
            (bool ok,) = address(vault).call{gas: g}(call);
            if (ok) {
                sawSuccess = true;
                assertEq(vault.pendingSlashTotal(), 0, "parked under a tight gas limit");
                assertEq(usdc.balanceOf(user), 2500 * USDC);
            }
            vm.revertToState(snap);
        }
        assertTrue(sawSuccess);
    }

    function test_ReturnBombDoesNotOOG() public {
        // Baseline: a token that burns the whole push budget and returns nothing.
        GasBurnerToken burner = new GasBurnerToken();
        AllwaysUsdcVault bv = _hostileVault(burner);
        burner.arm(true);
        uint256 g0 = gasleft();
        vm.prank(v2);
        bv.voteSlash{gas: 1_000_000}(bob, REF, 3000 * USDC, user, 2500 * USDC);
        uint256 burnerUsed = g0 - gasleft();

        ReturnBombToken t = new ReturnBombToken();
        AllwaysUsdcVault hv = _hostileVault(t);
        t.setMode(1);
        g0 = gasleft();
        vm.prank(v2);
        hv.voteSlash{gas: 1_000_000}(bob, REF, 3000 * USDC, user, 2500 * USDC);
        // Copying 200 KB would add ~115k gas on the caller's side; the capped copy adds nothing.
        assertLe(g0 - gasleft(), burnerUsed, "revert data was copied");
        assertEq(hv.pendingSlashTotal(), 2500 * USDC, "revert bomb parks");

        // A success bomb (first word 1) counts as paid, still without copying the payload.
        t.setMode(2);
        bytes32 ref2 = bytes32(uint256(0xdef));
        vm.prank(v1);
        hv.voteSlash(bob, ref2, 100 * USDC, carol, 100 * USDC);
        vm.prank(v2);
        hv.voteSlash{gas: 1_000_000}(bob, ref2, 100 * USDC, carol, 100 * USDC);
        assertEq(t.balanceOf(carol), 100 * USDC);
        assertEq(hv.pendingSlashTotal(), 2500 * USDC);
    }

    function test_PushSuccessNeedsEmptyOrExactlyTrue() public {
        OddReturnToken t = new OddReturnToken();
        AllwaysUsdcVault hv = new AllwaysUsdcVault(IERC20(address(t)), _set1(v1), _cfg(100));
        t.mint(bob, 5000 * USDC);
        vm.startPrank(bob);
        t.approve(address(hv), 5000 * USDC);
        hv.postCollateral(5000 * USDC);
        vm.stopPrank();

        // mode 1: no return data, like old-style tokens: paid.
        t.setMode(1);
        vm.prank(v1);
        hv.voteSlash(bob, bytes32(uint256(1)), 10 * USDC, user, 10 * USDC);
        assertEq(hv.pendingSlashTotal(), 0);
        // mode 2: one byte: refusal, parked (and no revert from a strict decode).
        t.setMode(2);
        vm.prank(v1);
        hv.voteSlash(bob, bytes32(uint256(2)), 10 * USDC, user, 10 * USDC);
        assertEq(hv.pendingSlashTotal(), 10 * USDC);
        // mode 3: a word that isn't a bool: refusal, parked.
        t.setMode(3);
        vm.prank(v1);
        hv.voteSlash(bob, bytes32(uint256(3)), 10 * USDC, user, 10 * USDC);
        assertEq(hv.pendingSlashTotal(), 20 * USDC);
    }

    function test_ReentrantTokenCannotReenter() public {
        ReentrantToken t = new ReentrantToken();
        AllwaysUsdcVault hv = new AllwaysUsdcVault(IERC20(address(t)), _set2(v1, address(t)), _cfg(100));
        t.setVault(hv);
        t.mint(bob, 5000 * USDC);
        vm.prank(bob);
        t.approve(address(hv), 5000 * USDC);

        t.arm(true);
        // transferFrom (post), transfer (withdraw) and transfer (claim) each re-enter all 15 entry points.
        vm.prank(bob);
        hv.postCollateral(5000 * USDC);
        vm.prank(bob);
        hv.withdrawCollateral(1000 * USDC);
        assertEq(t.attempts(), 30);
        assertEq(t.blockedByGuard(), 30);
        assertFalse(t.anySucceeded());

        // A slash push and the claim of a parked one.
        t.arm(false);
        vm.prank(v1);
        hv.voteSlash(bob, REF, 100 * USDC, user, 100 * USDC);
        t.arm(true);
        vm.prank(address(t));
        hv.voteSlash(bob, REF, 100 * USDC, user, 100 * USDC);
        assertTrue(hv.isSlashed(REF));
        assertFalse(t.anySucceeded());
        assertEq(hv.totalCollateral(), 3900 * USDC);
        assertGe(t.balanceOf(address(hv)), hv.totalCollateral() + hv.pendingSlashTotal());
        (address who,) = hv.pendingSlash(REF);
        if (who != address(0)) hv.claimSlash(REF);
        assertEq(t.blockedByGuard(), t.attempts());
        assertFalse(t.anySucceeded());
    }

    // ── issuer actions ──────────────────────────────────────────────────────

    function test_PausedTokenSlashParksWithdrawReverts() public {
        _post(bob, 5000 * USDC);
        _post(carol, 1000 * USDC);
        usdc.pause();
        _quorumSlash(bob, user, 3000 * USDC, 2500 * USDC);
        assertEq(vault.pendingSlashTotal(), 2500 * USDC);

        vm.prank(carol);
        vm.expectRevert("Pausable: paused");
        vault.withdrawCollateral(100 * USDC);
        assertEq(_collateral(carol), 1000 * USDC);

        usdc.mint(carol, 10 * USDC);
        vm.startPrank(carol);
        usdc.approve(address(vault), 10 * USDC);
        vm.expectRevert("Pausable: paused");
        vault.postCollateral(10 * USDC);
        vm.stopPrank();
        _assertBooks();

        usdc.unpause();
        vault.claimSlash(REF);
        assertEq(usdc.balanceOf(user), 2500 * USDC);
    }

    function test_VaultBlacklistedBooksStayConsistent() public {
        _post(bob, 5000 * USDC);
        _post(carol, 1000 * USDC);
        _lock(carol);
        usdc.mint(address(vault), 100 * USDC);
        usdc.blacklist(address(vault));

        _quorumSlash(bob, user, 3000 * USDC, 2500 * USDC);
        assertEq(vault.pendingSlashTotal(), 2500 * USDC, "push parks");

        AllwaysUsdcVault.FeeEntry[] memory e = _entries1(bob, 200 * USDC);
        _collect(v1, e);
        _collect(v2, e);
        assertEq(_collateral(bob), 1800 * USDC, "fee books still settle");

        bytes32 lot = bytes32(uint256(77));
        uint64 dl = uint64(block.timestamp + 600);
        vm.prank(v1);
        vault.votePayLot(lot, carol, 100 * USDC, dl);
        vm.prank(v2);
        vault.votePayLot(lot, carol, 100 * USDC, dl);
        assertEq(uint8(vault.lotStatus(lot)), uint8(AllwaysUsdcVault.LotStatus.Failed));

        vm.prank(v1);
        vault.voteUnlock(carol, 1);
        vm.prank(v2);
        vault.voteUnlock(carol, 1);
        vm.prank(carol);
        vm.expectRevert("Blacklistable: account is blacklisted");
        vault.withdrawCollateral(1000 * USDC);

        assertEq(vault.totalCollateral(), _collateral(bob) + _collateral(carol));
        _assertBooks();
        assertEq(vault.pot(), 100 * USDC + 500 * USDC + 200 * USDC);
    }

    // ── fee batch ordering ──────────────────────────────────────────────────

    function test_BatchMustBeStrictlyAscending() public {
        (address lo, address hi) = bob < carol ? (bob, carol) : (carol, bob);
        AllwaysUsdcVault.FeeEntry[] memory e = new AllwaysUsdcVault.FeeEntry[](2);
        e[0] = AllwaysUsdcVault.FeeEntry(hi, 1);
        e[1] = AllwaysUsdcVault.FeeEntry(lo, 1);
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.BatchNotSorted.selector);
        vault.voteCollectFeesBatch(e);
        e[0] = AllwaysUsdcVault.FeeEntry(lo, 1);
        e[1] = AllwaysUsdcVault.FeeEntry(lo, 2);
        vm.prank(v1);
        vm.expectRevert(AllwaysUsdcVault.BatchNotSorted.selector);
        vault.voteCollectFeesBatch(e);
        e[1] = AllwaysUsdcVault.FeeEntry(hi, 2);
        _collect(v1, e);
    }

    // ── Solana binding ──────────────────────────────────────────────────────

    function test_LockStoresSolanaPubkeyAndRejectsZero() public {
        _post(bob, 100 * USDC);
        vm.prank(bob);
        vm.expectRevert(AllwaysUsdcVault.InvalidSolanaPubkey.selector);
        vault.lockBond(bytes32(0));
        vm.expectEmit(address(vault));
        emit AllwaysUsdcVault.BondLockChanged(bob, true, 1, _pk(bob));
        _lock(bob);
        assertEq(vault.solanaPubkeyOf(bob), _pk(bob));
    }

    function test_RelockMayRebindPubkeyOnlyAfterUnlock() public {
        _post(bob, 100 * USDC);
        _lock(bob);
        vm.prank(bob);
        vm.expectRevert(AllwaysUsdcVault.LockStateUnchanged.selector);
        vault.lockBond(bytes32(uint256(2)));
        assertEq(vault.solanaPubkeyOf(bob), _pk(bob));

        vm.prank(v1);
        vault.voteUnlock(bob, 1);
        vm.prank(v2);
        vault.voteUnlock(bob, 1);
        assertEq(vault.solanaPubkeyOf(bob), _pk(bob), "unlock keeps the last binding");
        vm.prank(bob);
        vault.lockBond(bytes32(uint256(2)));
        assertEq(vault.solanaPubkeyOf(bob), bytes32(uint256(2)));
        (, bool locked, uint64 epoch,) = vault.getBond(bob);
        assertTrue(locked);
        assertEq(epoch, 3);
    }

    // ── no privileges, no ETH ───────────────────────────────────────────────

    function test_NoEthAccepted() public {
        vm.deal(carol, 1 ether);
        vm.prank(carol);
        (bool ok,) = address(vault).call{value: 1 ether}("");
        assertFalse(ok);
        vm.prank(carol);
        (ok,) = address(vault).call{value: 1 ether}(hex"deadbeef");
        assertFalse(ok);
        assertEq(address(vault).balance, 0);
    }

    function test_DeployerHasNoPrivileges() public {
        // No storage slot holds the deployer.
        for (uint256 slot; slot < 64; ++slot) {
            bytes32 word = vm.load(address(vault), bytes32(slot));
            assertTrue(address(uint160(uint256(word))) != deployer, "deployer stored");
        }
        _post(bob, 1000 * USDC);
        _lock(bob);
        vm.startPrank(deployer);
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.voteUnlock(bob, 1);
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.voteSlash(bob, REF, 1, deployer, 1);
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.voteCollectFeesBatch(_entries1(bob, 1));
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.votePayLot(REF, bob, 1, uint64(block.timestamp));
        vm.expectRevert(AllwaysUsdcVault.NotValidator.selector);
        vault.ping();
        vm.expectRevert(AllwaysUsdcVault.NotPendingValidator.selector);
        vault.acceptValidator();
        vm.expectRevert(AllwaysUsdcVault.InvalidAmount.selector);
        vault.withdrawCollateral(0);
        vm.stopPrank();
        assertFalse(vault.isValidator(deployer));
    }
}

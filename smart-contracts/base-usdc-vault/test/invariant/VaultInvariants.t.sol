// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Test, console2} from "forge-std/Test.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {AllwaysUsdcVault} from "../../src/AllwaysUsdcVault.sol";
import {MockFiatToken} from "../mocks/MockTokens.sol";
import {VaultHandler} from "./VaultHandler.sol";

contract VaultInvariants is Test {
    AllwaysUsdcVault internal vault;
    MockFiatToken internal usdc;
    VaultHandler internal handler;

    function setUp() public {
        usdc = new MockFiatToken();
        address[] memory pool = new address[](6);
        for (uint256 i; i < 6; ++i) {
            pool[i] = makeAddr(string.concat("validator", vm.toString(i)));
        }
        address[] memory seed = new address[](3);
        (seed[0], seed[1], seed[2]) = (pool[0], pool[1], pool[2]);
        vault = new AllwaysUsdcVault(
            IERC20(address(usdc)), seed, AllwaysUsdcVault.Config(10e6, 100_000e6, 66, 1800, 1000e6)
        );
        handler = new VaultHandler(vault, usdc, pool);
        targetContract(address(handler));
    }

    /// I1
    function invariant_Solvency() public view {
        assertGe(usdc.balanceOf(address(vault)), vault.totalCollateral() + vault.pendingSlashTotal());
    }

    /// I2
    function invariant_TotalCollateralMatchesSum() public view {
        uint256 sum;
        for (uint256 i; i < 4; ++i) {
            sum += vault.collateralOf(handler.miners(i));
        }
        assertEq(vault.totalCollateral(), sum);
    }

    /// I3
    function invariant_PendingSlashTotalMatchesSum() public view {
        uint256 sum;
        for (uint256 i; i < 24; ++i) {
            (, uint128 amt) = vault.pendingSlash(handler.refs(i));
            sum += amt;
        }
        assertEq(vault.pendingSlashTotal(), sum);
    }

    /// I4: bonds fall only by own withdraw while unlocked, a slash quorum or a fee quorum.
    function invariant_BondOnlyMovesByAllowedPaths() public view {
        assertFalse(handler.illegalBondMove());
    }

    /// I5: epochs never regress, bump by exactly one per transition, and unlock only by quorum or dead-man.
    function invariant_EpochMonotonic() public view {
        assertFalse(handler.epochRegressed());
        assertFalse(handler.illegalLockMove());
    }

    /// I6
    function invariant_MarkersOneShot() public view {
        assertFalse(handler.markerViolation());
    }

    /// I7: the pot leaves only through paid lots, each within cap, pot, bond and coverage; every
    /// balance move is explained by a deposit, withdraw, reimbursement, claim, lot or donation.
    function invariant_PotOutflowBounded() public view {
        assertFalse(handler.potOutflowViolation());
        assertFalse(handler.unexplainedBalanceMove());
        assertEq(usdc.balanceOf(address(vault)), handler.expectedBalance());
        assertEq(vault.totalLotsPaid(), handler.ghostLotsPaid());
    }

    /// I8
    function invariant_SettledMonotonic() public view {
        assertFalse(handler.settledRegressed());
    }

    /// I9
    function invariant_ValidatorSetWellFormed() public view {
        address[] memory set = vault.getValidators();
        assertGe(set.length, 2); // seeded at 3; removal is refused below 3
        assertLe(set.length + vault.getPendingValidators().length, 16);
        for (uint256 i; i < set.length; ++i) {
            assertTrue(vault.isValidator(set[i]));
            for (uint256 j = i + 1; j < set.length; ++j) {
                assertTrue(set[i] != set[j]);
            }
        }
        for (uint256 i; i < 6; ++i) {
            address p = handler.pool(i);
            bool inSet;
            for (uint256 j; j < set.length; ++j) {
                if (set[j] == p) inSet = true;
            }
            assertEq(vault.isValidator(p), inSet);
        }
    }

    /// I10
    function invariant_ConfigWithinBounds() public view {
        AllwaysUsdcVault.Config memory c = vault.getConfig();
        assertTrue(c.minCollateral >= 1e6 && c.minCollateral <= 1e12);
        assertTrue(c.maxCollateral == 0 || (c.maxCollateral >= c.minCollateral && c.maxCollateral <= 1e13));
        assertTrue(c.consensusThresholdPct >= 51 && c.consensusThresholdPct <= 100);
        assertTrue(c.voteRoundTtl >= 600 && c.voteRoundTtl <= 86_400);
        assertTrue(c.maxLotAmount >= 1e6 && c.maxLotAmount <= 1e10);
    }

    /// I13: the lot singleton and its one round exist together or not at all.
    function invariant_SingleOpenLot() public view {
        (bytes32 openRef,) = vault.openLot();
        (, uint64 id,,) = vault.getRound(vault.roundKeyForPayLot());
        assertEq(openRef != bytes32(0), id != 0);
    }

    function afterInvariant() external view {
        console2.log("slash quorums", handler.slashQuorums(), "parked", handler.parkedSlashes());
        console2.log("lots paid", handler.lotsPaid(), "failed", handler.lotsFailed());
        console2.log("unlock quorums", handler.unlockQuorums(), "self-unlocks", handler.selfUnlocks());
        console2.log(
            "membership changes", handler.membershipChanges(), "config changes", handler.configChanges()
        );
    }
}

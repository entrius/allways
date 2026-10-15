// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {AllwaysUsdcVault} from "../src/AllwaysUsdcVault.sol";
import {VaultTestBase} from "./VaultTestBase.sol";
import {EighteenDecimalToken} from "./mocks/MockTokens.sol";

/// Every governable field against its compile-time bounds, in the constructor and in a config vote.
contract ConfigTest is VaultTestBase {
    function _accepts(AllwaysUsdcVault.Config memory c) internal returns (bool) {
        try new AllwaysUsdcVault(IERC20(address(usdc)), _set1(v1), c) {
            return true;
        } catch {
            return false;
        }
    }

    function _expectField(AllwaysUsdcVault.Config memory c, uint8 field) internal {
        vm.expectRevert(abi.encodeWithSelector(AllwaysUsdcVault.ConfigOutOfBounds.selector, field));
        new AllwaysUsdcVault(IERC20(address(usdc)), _set1(v1), c);
        vm.prank(v1);
        vm.expectRevert(abi.encodeWithSelector(AllwaysUsdcVault.ConfigOutOfBounds.selector, field));
        vault.voteSetConfig(c);
    }

    function _ok(AllwaysUsdcVault.Config memory c) internal {
        assertTrue(_accepts(c));
        AllwaysUsdcVault vv = new AllwaysUsdcVault(IERC20(address(usdc)), _set1(v1), _cfg(100));
        vm.prank(v1);
        vv.voteSetConfig(c);
        assertEq(abi.encode(vv.getConfig()), abi.encode(c));
    }

    function test_MinCollateralBounds() public {
        AllwaysUsdcVault.Config memory c = _cfg(66);
        c.maxCollateral = 0;
        c.minCollateral = 1e6 - 1;
        _expectField(c, 0);
        c.minCollateral = 1e6;
        _ok(c);
        c.minCollateral = 1e6 + 1;
        _ok(c);
        c.minCollateral = 1e12 - 1;
        _ok(c);
        c.minCollateral = 1e12;
        _ok(c);
        c.minCollateral = 1e12 + 1;
        _expectField(c, 0);
    }

    function test_MaxCollateralBounds() public {
        AllwaysUsdcVault.Config memory c = _cfg(66);
        c.maxCollateral = 0;
        _ok(c);
        c.maxCollateral = c.minCollateral - 1;
        _expectField(c, 1);
        c.maxCollateral = c.minCollateral;
        _ok(c);
        c.maxCollateral = c.minCollateral + 1;
        _ok(c);
        c.maxCollateral = 1e13 - 1;
        _ok(c);
        c.maxCollateral = 1e13;
        _ok(c);
        c.maxCollateral = 1e13 + 1;
        _expectField(c, 1);
    }

    function test_ThresholdBounds() public {
        AllwaysUsdcVault.Config memory c = _cfg(50);
        _expectField(c, 2);
        c.consensusThresholdPct = 51;
        _ok(c);
        c.consensusThresholdPct = 52;
        _ok(c);
        c.consensusThresholdPct = 99;
        _ok(c);
        c.consensusThresholdPct = 100;
        _ok(c);
        c.consensusThresholdPct = 101;
        _expectField(c, 2);
    }

    function test_TtlBounds() public {
        AllwaysUsdcVault.Config memory c = _cfg(66);
        c.voteRoundTtl = 599;
        _expectField(c, 3);
        c.voteRoundTtl = 600;
        _ok(c);
        c.voteRoundTtl = 601;
        _ok(c);
        c.voteRoundTtl = 86_399;
        _ok(c);
        c.voteRoundTtl = 86_400;
        _ok(c);
        c.voteRoundTtl = 86_401;
        _expectField(c, 3);
    }

    function test_MaxLotAmountBounds() public {
        AllwaysUsdcVault.Config memory c = _cfg(66);
        c.maxLotAmount = 1e6 - 1;
        _expectField(c, 4);
        c.maxLotAmount = 1e6;
        _ok(c);
        c.maxLotAmount = 1e6 + 1;
        _ok(c);
        c.maxLotAmount = 1e10 - 1;
        _ok(c);
        c.maxLotAmount = 1e10;
        _ok(c);
        c.maxLotAmount = 1e10 + 1;
        _expectField(c, 4);
    }

    function testFuzz_ConfigBoundsEveryField(
        uint128 minC,
        uint128 maxC,
        uint8 threshold,
        uint32 ttl,
        uint128 maxLot
    ) public {
        AllwaysUsdcVault.Config memory c = AllwaysUsdcVault.Config(minC, maxC, threshold, ttl, maxLot);
        bool inBounds = minC >= 1e6 && minC <= 1e12 && (maxC == 0 || (maxC >= minC && maxC <= 1e13))
            && threshold >= 51 && threshold <= 100 && ttl >= 600 && ttl <= 86_400 && maxLot >= 1e6
            && maxLot <= 1e10;
        assertEq(_accepts(c), inBounds);
        vm.prank(v1);
        if (inBounds) {
            vault.voteSetConfig(c);
        } else {
            vm.expectRevert();
            vault.voteSetConfig(c);
        }
    }
}

/// Constructor refusals observed through try/catch, so the reverting deploys are visible to coverage.
contract ConstructorRefusalTest is VaultTestBase {
    function _refusal(IERC20 t, address[] memory vals) internal returns (bytes4 sel) {
        try new AllwaysUsdcVault(t, vals, _cfg(100)) {
            return bytes4(0);
        } catch (bytes memory err) {
            return bytes4(err);
        }
    }

    function test_ConstructorRefusalsByTryCatch() public {
        IERC20 t = IERC20(address(usdc));
        assertEq(_refusal(IERC20(makeAddr("eoa")), _set1(v1)), AllwaysUsdcVault.InvalidToken.selector);
        assertEq(
            _refusal(IERC20(address(new EighteenDecimalToken())), _set1(v1)),
            AllwaysUsdcVault.InvalidToken.selector
        );
        assertEq(_refusal(t, _set2(v1, address(0))), AllwaysUsdcVault.InvalidValidatorSet.selector);
        assertEq(_refusal(t, _set2(v1, v1)), AllwaysUsdcVault.InvalidValidatorSet.selector);
        address predicted = vm.computeCreateAddress(address(this), vm.getNonce(address(this)));
        assertEq(_refusal(t, _set2(v1, predicted)), AllwaysUsdcVault.InvalidValidatorSet.selector);
        assertEq(_refusal(t, _set1(v1)), bytes4(0));
    }
}

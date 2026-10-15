// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Test} from "forge-std/Test.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {AllwaysUsdcVault} from "../src/AllwaysUsdcVault.sol";
import {MockFiatToken} from "./mocks/MockTokens.sol";

abstract contract VaultTestBase is Test {
    uint256 internal constant USDC = 1e6;
    uint128 internal constant MIN_COLLATERAL = 10e6;

    MockFiatToken internal usdc;
    AllwaysUsdcVault internal vault;

    address internal v1 = makeAddr("v1");
    address internal v2 = makeAddr("v2");
    address internal v3 = makeAddr("v3");
    address internal v4 = makeAddr("v4");
    address internal bob;
    uint256 internal bobPk;
    address internal carol = makeAddr("carol");
    address internal user = makeAddr("user");
    address internal frank = makeAddr("frank");
    address internal deployer = makeAddr("deployer");

    function setUp() public virtual {
        (bob, bobPk) = makeAddrAndKey("bob");
        usdc = new MockFiatToken();
        vault = _deploy(_set2(v1, v2), 100);
    }

    function _cfg(uint8 threshold) internal pure returns (AllwaysUsdcVault.Config memory) {
        return AllwaysUsdcVault.Config({
            minCollateral: MIN_COLLATERAL,
            maxCollateral: 100_000e6,
            consensusThresholdPct: threshold,
            voteRoundTtl: 1800,
            maxLotAmount: 1000e6
        });
    }

    function _clone(AllwaysUsdcVault.Config memory c) internal pure returns (AllwaysUsdcVault.Config memory) {
        return AllwaysUsdcVault.Config(
            c.minCollateral, c.maxCollateral, c.consensusThresholdPct, c.voteRoundTtl, c.maxLotAmount
        );
    }

    function _deploy(address[] memory vals, uint8 threshold) internal returns (AllwaysUsdcVault) {
        vm.prank(deployer);
        return new AllwaysUsdcVault(IERC20(address(usdc)), vals, _cfg(threshold));
    }

    function _set1(address a) internal pure returns (address[] memory s) {
        s = new address[](1);
        s[0] = a;
    }

    function _set2(address a, address b) internal pure returns (address[] memory s) {
        s = new address[](2);
        (s[0], s[1]) = (a, b);
    }

    function _set3(address a, address b, address c) internal pure returns (address[] memory s) {
        s = new address[](3);
        (s[0], s[1], s[2]) = (a, b, c);
    }

    function _post(address m, uint256 amount) internal {
        usdc.mint(m, amount);
        vm.startPrank(m);
        usdc.approve(address(vault), amount);
        vault.postCollateral(amount);
        vm.stopPrank();
    }

    function _pk(address m) internal pure returns (bytes32) {
        return keccak256(abi.encode("solana", m));
    }

    function _lock(address m) internal {
        vm.prank(m);
        vault.lockBond(_pk(m));
    }

    function _collateral(address m) internal view returns (uint256) {
        return vault.collateralOf(m);
    }

    function _locked(address m) internal view returns (bool locked, uint64 epoch) {
        (, locked, epoch,) = vault.getBond(m);
    }

    function _slash(address val, address m, bytes32 ref, uint256 penalty, address payee, uint256 reimb)
        internal
    {
        vm.prank(val);
        vault.voteSlash(m, ref, penalty, payee, reimb);
    }

    function _entries1(address m, uint256 cum) internal pure returns (AllwaysUsdcVault.FeeEntry[] memory e) {
        e = new AllwaysUsdcVault.FeeEntry[](1);
        e[0] = AllwaysUsdcVault.FeeEntry(m, cum);
    }

    /// Two entries in ascending address order, whichever way round the arguments come.
    function _entries2(address a, uint256 ca, address b, uint256 cb)
        internal
        pure
        returns (AllwaysUsdcVault.FeeEntry[] memory e)
    {
        e = new AllwaysUsdcVault.FeeEntry[](2);
        if (a < b) {
            (e[0], e[1]) = (AllwaysUsdcVault.FeeEntry(a, ca), AllwaysUsdcVault.FeeEntry(b, cb));
        } else {
            (e[0], e[1]) = (AllwaysUsdcVault.FeeEntry(b, cb), AllwaysUsdcVault.FeeEntry(a, ca));
        }
    }

    function _collect(address val, AllwaysUsdcVault.FeeEntry[] memory e) internal {
        vm.prank(val);
        vault.voteCollectFeesBatch(e);
    }

    function _assertBooks() internal view {
        assertGe(
            usdc.balanceOf(address(vault)), vault.totalCollateral() + vault.pendingSlashTotal(), "solvency"
        );
    }
}

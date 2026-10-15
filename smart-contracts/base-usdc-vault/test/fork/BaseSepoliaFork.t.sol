// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Test} from "forge-std/Test.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Permit} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Permit.sol";
import {AllwaysUsdcVault} from "../../src/AllwaysUsdcVault.sol";

interface IFiatToken {
    function paused() external view returns (bool);
    function isBlacklisted(address) external view returns (bool);
}

/// Every path against Circle's USDC on a Base Sepolia fork. Skipped unless BASE_SEPOLIA_RPC_URL is set.
/// Run with FOUNDRY_PROFILE=gas to record per-call gas against the real FiatToken.
contract BaseSepoliaForkTest is Test {
    IERC20 internal constant USDC = IERC20(0x036CbD53842c5426634e7929541eC2318f3dCF7e);

    AllwaysUsdcVault internal vault;
    address internal v1 = makeAddr("v1");
    address internal v2 = makeAddr("v2");
    address internal v3 = makeAddr("v3");
    address internal miner;
    uint256 internal minerPk;
    address internal user = makeAddr("user");

    function setUp() public {
        string memory rpc = vm.envOr("BASE_SEPOLIA_RPC_URL", string(""));
        if (bytes(rpc).length == 0) {
            vm.skip(true);
            return;
        }
        vm.createSelectFork(rpc);
        (miner, minerPk) = makeAddrAndKey("miner");
        address[] memory vals = new address[](3);
        (vals[0], vals[1], vals[2]) = (v1, v2, v3);
        vault = new AllwaysUsdcVault(USDC, vals, AllwaysUsdcVault.Config(10e6, 100_000e6, 66, 1800, 1000e6));
        deal(address(USDC), miner, 5000e6);
    }

    function _snap(string memory name) internal {
        vm.snapshotGasLastCall("fork", name);
    }

    function testFork_BaseSepoliaUsdc() public {
        assertFalse(IFiatToken(address(USDC)).paused());
        assertFalse(IFiatToken(address(USDC)).isBlacklisted(address(vault)));

        // Plain post.
        vm.prank(miner);
        USDC.approve(address(vault), 1000e6);
        vm.prank(miner);
        vault.postCollateral(1000e6);
        _snap("postCollateral_first");

        // EIP-2612 deposit in one tx, against the real token's domain.
        uint256 deadline = block.timestamp + 1 hours;
        bytes32 structHash = keccak256(
            abi.encode(
                keccak256(
                    "Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)"
                ),
                miner,
                address(vault),
                uint256(2000e6),
                IERC20Permit(address(USDC)).nonces(miner),
                deadline
            )
        );
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(
            minerPk,
            keccak256(
                abi.encodePacked("\x19\x01", IERC20Permit(address(USDC)).DOMAIN_SEPARATOR(), structHash)
            )
        );
        vm.prank(miner);
        vault.postCollateralWithPermit(2000e6, deadline, v, r, s);
        _snap("postCollateralWithPermit");
        assertEq(vault.collateralOf(miner), 3000e6);
        assertEq(USDC.balanceOf(address(vault)), 3000e6);

        // Lock, then a slash that pushes the reimbursement.
        vm.prank(miner);
        vault.lockBond(bytes32(uint256(0x501)));
        _snap("lockBond_first");
        bytes32 ref = bytes32(uint256(0xabc));
        vm.prank(v1);
        vault.voteSlash(miner, ref, 500e6, user, 400e6);
        _snap("voteSlash_open");
        vm.prank(v2);
        vault.voteSlash(miner, ref, 500e6, user, 400e6);
        _snap("voteSlash_quorum_pushed");
        assertEq(USDC.balanceOf(user), 400e6);
        assertEq(vault.pendingSlashTotal(), 0);
        assertEq(vault.pot(), 100e6);

        // Donate to the pot and pay a lot to the bond address.
        deal(address(USDC), address(this), 1000e6);
        USDC.transfer(address(vault), 1000e6);
        assertEq(vault.pot(), 1100e6);
        bytes32 lot = bytes32(uint256(0x107));
        uint64 dl = uint64(block.timestamp + 1800);
        uint256 before = USDC.balanceOf(miner);
        vm.prank(v1);
        vault.votePayLot(lot, miner, 1000e6, dl);
        _snap("votePayLot_open");
        vm.prank(v2);
        vault.votePayLot(lot, miner, 1000e6, dl);
        _snap("votePayLot_quorum_paid");
        assertEq(USDC.balanceOf(miner), before + 1000e6);
        assertEq(uint8(vault.lotStatus(lot)), uint8(AllwaysUsdcVault.LotStatus.Paid));

        // Unlock by quorum and withdraw everything.
        vm.prank(v1);
        vault.voteUnlock(miner, 1);
        vm.prank(v2);
        vault.voteUnlock(miner, 1);
        _snap("voteUnlock_quorum");
        uint256 c = vault.collateralOf(miner);
        vm.prank(miner);
        vault.withdrawCollateral(c);
        _snap("withdrawCollateral");
        assertEq(vault.totalCollateral(), 0);
        assertEq(USDC.balanceOf(address(vault)), vault.pot());
    }
}

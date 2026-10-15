// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Script, console2} from "forge-std/Script.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {AllwaysUsdcVault} from "../src/AllwaysUsdcVault.sol";

/// Deploys the vault and checks the result.
///   USDC_ADDRESS     token (required)
///   VALIDATORS       comma-separated validator EOAs (required)
///   MIN_COLLATERAL, MAX_COLLATERAL, THRESHOLD_PCT, VOTE_ROUND_TTL, MAX_LOT_AMOUNT
///                    optional; default to the launch config (10 / 100k USDC, 66%, 1800 s, 1k USDC)
contract Deploy is Script {
    function run() external returns (AllwaysUsdcVault vault) {
        IERC20 usdc = IERC20(vm.envAddress("USDC_ADDRESS"));
        address[] memory validators = vm.envAddress("VALIDATORS", ",");
        AllwaysUsdcVault.Config memory cfg = AllwaysUsdcVault.Config({
            minCollateral: uint128(vm.envOr("MIN_COLLATERAL", uint256(10e6))),
            maxCollateral: uint128(vm.envOr("MAX_COLLATERAL", uint256(100_000e6))),
            consensusThresholdPct: uint8(vm.envOr("THRESHOLD_PCT", uint256(66))),
            voteRoundTtl: uint32(vm.envOr("VOTE_ROUND_TTL", uint256(1800))),
            maxLotAmount: uint128(vm.envOr("MAX_LOT_AMOUNT", uint256(1000e6)))
        });

        vm.startBroadcast();
        vault = new AllwaysUsdcVault(usdc, validators, cfg);
        vm.stopBroadcast();

        _check(vault, usdc, validators, cfg);
        console2.log("AllwaysUsdcVault", address(vault));
        console2.log("chain id", block.chainid);
        console2.log("validators", validators.length, "required votes", vault.requiredVotes());
        console2.log("pre-existing USDC at the address (pot)", vault.pot());
    }

    /// The deployed state must be exactly what was asked for.
    function _check(
        AllwaysUsdcVault vault,
        IERC20 usdc,
        address[] memory validators,
        AllwaysUsdcVault.Config memory cfg
    ) internal view {
        require(vault.VERSION() == 1, "version");
        require(address(vault.usdc()) == address(usdc), "usdc");
        require(
            keccak256(abi.encode(vault.getValidators())) == keccak256(abi.encode(validators)), "validators"
        );
        require(keccak256(abi.encode(vault.getConfig())) == keccak256(abi.encode(cfg)), "config");
        // Books start empty, so any USDC already at the address (anyone can pre-send) is pot.
        require(vault.totalCollateral() == 0 && vault.pendingSlashTotal() == 0, "books");
        require(vault.pot() == usdc.balanceOf(address(vault)), "pot");
    }
}

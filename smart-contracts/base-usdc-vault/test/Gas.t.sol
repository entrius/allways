// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {AllwaysUsdcVault} from "../src/AllwaysUsdcVault.sol";
import {VaultTestBase} from "./VaultTestBase.sol";

/// Per-call gas for each vote shape, recorded to snapshots/VaultGasTest.json. Run with --isolate
/// (FOUNDRY_PROFILE=gas) so every call is its own transaction with cold storage.
/// Five validators at 66%: a round needs 4 votes, so "open", "join" and "quorum" are distinct.
contract VaultGasTest is VaultTestBase {
    address internal v5 = makeAddr("v5");
    address[] internal five;

    function setUp() public override {
        super.setUp();
        five = [v1, v2, v3, v4, v5];
        vault = _deploy(five, 66);
        _post(bob, 5000 * USDC);
        _lock(bob);
        _post(carol, 5000 * USDC);
        usdc.mint(address(vault), 3000 * USDC);
    }

    function _snap(string memory name) internal {
        vm.snapshotGasLastCall("vault", name);
    }

    function test_GasBonds() public {
        usdc.mint(frank, 2000 * USDC);
        vm.prank(frank);
        usdc.approve(address(vault), type(uint256).max);
        vm.prank(frank);
        vault.postCollateral(1000 * USDC);
        _snap("postCollateral_first");
        vm.prank(frank);
        vault.postCollateral(1000 * USDC);
        _snap("postCollateral_topUp");
        vm.prank(frank);
        vault.lockBond(_pk(frank));
        _snap("lockBond_first");
        vm.prank(carol);
        vault.withdrawCollateral(1000 * USDC);
        _snap("withdrawCollateral");
        vm.prank(v1);
        vault.ping();
        _snap("ping");
        skip(15 days);
        vm.prank(bob);
        vault.selfUnlock();
        _snap("selfUnlock");
    }

    function test_GasUnlock() public {
        vm.prank(v1);
        vault.voteUnlock(bob, 1);
        _snap("voteUnlock_open");
        vm.prank(v2);
        vault.voteUnlock(bob, 1);
        _snap("voteUnlock_join");
        vm.prank(v3);
        vault.voteUnlock(bob, 1);
        vm.prank(v4);
        vault.voteUnlock(bob, 1);
        _snap("voteUnlock_quorum");
    }

    function _slashRound(bytes32 ref, address payee, string memory quorumName, bool recordVotes) internal {
        for (uint256 i; i < 4; ++i) {
            vm.prank(five[i]);
            vault.voteSlash(bob, ref, 1000 * USDC, payee, 800 * USDC);
            if (recordVotes && i == 0) _snap("voteSlash_open");
            if (recordVotes && i == 1) _snap("voteSlash_join");
        }
        _snap(quorumName);
    }

    function test_GasSlash() public {
        _slashRound(bytes32(uint256(1)), user, "voteSlash_quorum_pushed", true);
        usdc.blacklist(frank);
        _slashRound(bytes32(uint256(2)), frank, "voteSlash_quorum_parked", false);
        _slashRound(bytes32(uint256(3)), address(vault), "voteSlash_quorum_payeeVault", false);
        usdc.unBlacklist(frank);
        vault.claimSlash(bytes32(uint256(2)));
        _snap("claimSlash");
    }

    /// n bonded miners at ascending addresses, each settling 1 USDC: every entry moves (worst case).
    function _batch(uint256 n) internal returns (AllwaysUsdcVault.FeeEntry[] memory e) {
        e = new AllwaysUsdcVault.FeeEntry[](n);
        for (uint256 i; i < n; ++i) {
            address m = address(uint160(0x10000 + i));
            _post(m, 20 * USDC);
            e[i] = AllwaysUsdcVault.FeeEntry(m, 1e6);
        }
    }

    function _collectRound(uint256 n) internal {
        AllwaysUsdcVault.FeeEntry[] memory e = _batch(n);
        for (uint256 i; i < 4; ++i) {
            vm.prank(five[i]);
            vault.voteCollectFeesBatch(e);
            if (i == 0) _snap(string.concat("voteCollectFeesBatch_", vm.toString(n), "_open"));
        }
        _snap(string.concat("voteCollectFeesBatch_", vm.toString(n), "_quorum"));
    }

    function test_GasCollect1() public {
        _collectRound(1);
    }

    function test_GasCollect64() public {
        _collectRound(64);
    }

    function test_GasCollect256() public {
        _collectRound(256);
    }

    function test_GasPayLot() public {
        bytes32 lot = bytes32(uint256(0x107));
        uint64 dl = uint64(block.timestamp + 1800);
        for (uint256 i; i < 4; ++i) {
            vm.prank(five[i]);
            vault.votePayLot(lot, bob, 500 * USDC, dl);
            if (i == 0) _snap("votePayLot_open");
            if (i == 1) _snap("votePayLot_join");
        }
        _snap("votePayLot_quorum_paid");

        usdc.blacklist(bob);
        bytes32 lot2 = bytes32(uint256(0x108));
        for (uint256 i; i < 4; ++i) {
            vm.prank(five[i]);
            vault.votePayLot(lot2, bob, 500 * USDC, dl);
        }
        _snap("votePayLot_quorum_failed");
    }

    function test_GasGovernance() public {
        address cand = makeAddr("cand");
        for (uint256 i; i < 5; ++i) {
            vm.prank(five[i]);
            vault.voteAddValidator(cand);
            if (i == 0) _snap("voteAddValidator_open");
        }
        _snap("voteAddValidator_quorum");
        vm.prank(cand);
        vault.acceptValidator();
        _snap("acceptValidator");

        // Six validators now; removal needs the other five.
        for (uint256 i; i < 5; ++i) {
            vm.prank(five[i]);
            vault.voteRemoveValidator(cand);
            if (i == 0) _snap("voteRemoveValidator_open");
        }
        _snap("voteRemoveValidator_quorum");

        AllwaysUsdcVault.Config memory cfg = _cfg(51);
        for (uint256 i; i < 5; ++i) {
            vm.prank(five[i]);
            vault.voteSetConfig(cfg);
            if (i == 0) _snap("voteSetConfig_open");
        }
        _snap("voteSetConfig_quorum");
    }
}

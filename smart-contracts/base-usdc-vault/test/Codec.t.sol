// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {AllwaysUsdcVault} from "../src/AllwaysUsdcVault.sol";
import {MockFiatToken} from "./mocks/MockTokens.sol";
import {Test} from "forge-std/Test.sol";

/// Round-hash vectors for an off-chain codec to match byte for byte. The committed JSON is checked
/// against fresh encodings and against the hash the vault stores on a real vote.
/// Regenerate with: UPDATE_VECTORS=1 forge test --match-test testCodecParity
contract CodecTest is Test {
    string internal constant PATH = "test/vectors/round_hashes.json";

    address internal constant V1 = 0x1111111111111111111111111111111111111111;
    address internal constant V2 = 0x2222222222222222222222222222222222222222;
    address internal constant V3 = 0x3333333333333333333333333333333333333333;
    address internal constant MINER = 0x00000000000000000000000000000000000B0b00;
    address internal constant USER = 0x000000000000000000000000000000000000cafE;
    address internal constant CANDIDATE = 0x4444444444444444444444444444444444444444;
    bytes32 internal constant SWAP_REF = 0x5157a9c5f00000000000000000000000000000000000000000000000000000aa;
    bytes32 internal constant LOT_REF = 0x107f0000000000000000000000000000000000000000000000000000000000bb;

    MockFiatToken internal usdc;
    AllwaysUsdcVault internal vault;

    function setUp() public {
        vm.warp(1_800_000_000);
        usdc = new MockFiatToken();
        address[] memory vals = new address[](3);
        (vals[0], vals[1], vals[2]) = (V1, V2, V3);
        vault = new AllwaysUsdcVault(IERC20(address(usdc)), vals, _cfg());
        usdc.mint(MINER, 5000e6);
        vm.startPrank(MINER);
        usdc.approve(address(vault), 5000e6);
        vault.postCollateral(5000e6);
        vault.lockBond(bytes32(uint256(0x50)));
        vm.stopPrank();
        usdc.mint(address(vault), 2000e6);
    }

    function _cfg() internal pure returns (AllwaysUsdcVault.Config memory) {
        return AllwaysUsdcVault.Config(10e6, 100_000e6, 66, 1800, 1000e6);
    }

    function _batch(uint256 n) internal pure returns (AllwaysUsdcVault.FeeEntry[] memory e) {
        e = new AllwaysUsdcVault.FeeEntry[](n);
        for (uint256 i; i < n; ++i) {
            e[i] = AllwaysUsdcVault.FeeEntry(address(uint160(0x10000 + i)), (i + 1) * 1_234_567);
        }
    }

    function _vector(string memory key, bytes memory encoded) internal returns (string memory) {
        vm.serializeBytes(key, "encoded", encoded);
        return vm.serializeBytes32(key, "hash", keccak256(encoded));
    }

    function _storedHash(bytes32 roundKey) internal view returns (bytes32 h) {
        (h,,,) = vault.getRound(roundKey);
    }

    function _deadline() internal view returns (uint64) {
        return uint64(block.timestamp + 1800);
    }

    /// Every round kind: inputs → abi.encode → keccak, plus the subject keys.
    function _buildHashes(bytes32 set) internal returns (string memory json) {
        string memory root = "root";
        vm.serializeBytes32(root, "setHash", set);
        vm.serializeBytes(root, "setHashEncoded", abi.encode(vault.getValidators()));
        vm.serializeString(root, "unlockKey", _vector("uk", abi.encode(uint8(0), MINER)));
        vm.serializeString(root, "unlock", _vector("u", abi.encode(uint8(0), MINER, uint64(1))));
        vm.serializeString(
            root,
            "slash",
            _vector("s", abi.encode(uint8(1), MINER, SWAP_REF, uint256(3000e6), USER, uint256(2500e6)))
        );
        vm.serializeString(root, "collect2", _vector("c2", abi.encode(uint8(2), _batch(2))));
        vm.serializeString(root, "collect256", _vector("c256", abi.encode(uint8(2), _batch(256))));
        vm.serializeString(root, "payLotKey", _vector("lk", abi.encode(uint8(7))));
        vm.serializeString(
            root, "payLot", _vector("l", abi.encode(uint8(7), LOT_REF, MINER, uint256(500e6), _deadline()))
        );
        vm.serializeString(root, "addKey", _vector("ak", abi.encode(uint8(3), CANDIDATE)));
        vm.serializeString(root, "add", _vector("a", abi.encode(uint8(3), CANDIDATE, set)));
        vm.serializeString(root, "removeKey", _vector("rk", abi.encode(uint8(4), V3)));
        vm.serializeString(root, "remove", _vector("r", abi.encode(uint8(4), V3, set)));
        json = vm.serializeString(root, "config", _vector("cf", abi.encode(uint8(5), _cfg(), set)));
    }

    /// The inputs, so the vectors are self-describing.
    function _buildInputs() internal returns (string memory) {
        AllwaysUsdcVault.Config memory cfg = _cfg();
        string memory inputs = "inputs";
        vm.serializeAddress(inputs, "validators", vault.getValidators());
        vm.serializeAddress(inputs, "miner", MINER);
        vm.serializeAddress(inputs, "user", USER);
        vm.serializeAddress(inputs, "candidate", CANDIDATE);
        vm.serializeAddress(inputs, "removeTarget", V3);
        vm.serializeBytes32(inputs, "swapRef", SWAP_REF);
        vm.serializeBytes32(inputs, "lotRef", LOT_REF);
        vm.serializeUint(inputs, "reqUnlock", 0);
        vm.serializeUint(inputs, "reqSlash", 1);
        vm.serializeUint(inputs, "reqCollect", 2);
        vm.serializeUint(inputs, "reqAddValidator", 3);
        vm.serializeUint(inputs, "reqRemoveValidator", 4);
        vm.serializeUint(inputs, "reqConfig", 5);
        vm.serializeUint(inputs, "reqPayLot", 7);
        vm.serializeUint(inputs, "unlockEpoch", 1);
        vm.serializeUint(inputs, "slashPenalty", 3000e6);
        vm.serializeUint(inputs, "slashReimbursement", 2500e6);
        vm.serializeUint(inputs, "lotAmount", 500e6);
        vm.serializeUint(inputs, "lotDeadline", _deadline());
        vm.serializeString(
            inputs, "batchRule", "entry i (0-based): miner = 0x10000 + i, cumulative = (i + 1) * 1234567"
        );
        vm.serializeUint(inputs, "configMinCollateral", cfg.minCollateral);
        vm.serializeUint(inputs, "configMaxCollateral", cfg.maxCollateral);
        vm.serializeUint(inputs, "configThresholdPct", cfg.consensusThresholdPct);
        vm.serializeUint(inputs, "configVoteRoundTtl", cfg.voteRoundTtl);
        return vm.serializeUint(inputs, "configMaxLotAmount", cfg.maxLotAmount);
    }

    function _h(string memory json, string memory key) internal pure returns (bytes32) {
        return vm.parseJsonBytes32(json, string.concat(".", key, ".hash"));
    }

    function testCodecParity() public {
        bytes32 set = vault.setHash();
        _buildHashes(set);
        string memory json = vm.serializeString("root", "inputs", _buildInputs());

        if (vm.envOr("UPDATE_VECTORS", false)) vm.writeJson(json, PATH);
        string memory c = vm.readFile(PATH);
        string[12] memory keys = [
            "unlockKey",
            "unlock",
            "slash",
            "collect2",
            "collect256",
            "payLotKey",
            "payLot",
            "addKey",
            "add",
            "removeKey",
            "remove",
            "config"
        ];
        for (uint256 i; i < keys.length; ++i) {
            assertEq(_h(c, keys[i]), _h(json, keys[i]), string.concat("stale vector: ", keys[i]));
            assertEq(
                vm.parseJsonBytes(c, string.concat(".", keys[i], ".encoded")),
                vm.parseJsonBytes(json, string.concat(".", keys[i], ".encoded"))
            );
        }
        assertEq(vm.parseJsonBytes32(c, ".setHash"), set);

        // The vault's helpers and the hashes it stores on real votes match the vectors.
        AllwaysUsdcVault.Config memory cfg = _cfg();
        vm.startPrank(V1);
        vault.voteUnlock(MINER, 1);
        vault.voteSlash(MINER, SWAP_REF, 3000e6, USER, 2500e6);
        vault.voteCollectFeesBatch(_batch(2));
        vault.voteCollectFeesBatch(_batch(256));
        vault.votePayLot(LOT_REF, MINER, 500e6, _deadline());
        vault.voteAddValidator(CANDIDATE);
        vault.voteRemoveValidator(V3);
        vault.voteSetConfig(cfg);
        vm.stopPrank();

        assertEq(vault.roundKeyForUnlock(MINER), _h(c, "unlockKey"));
        assertEq(_storedHash(_h(c, "unlockKey")), _h(c, "unlock"));
        assertEq(vault.roundKeyForSlash(MINER, SWAP_REF, 3000e6, USER, 2500e6), _h(c, "slash"));
        assertEq(_storedHash(_h(c, "slash")), _h(c, "slash"));
        assertEq(vault.roundKeyForCollect(_batch(2)), _h(c, "collect2"));
        assertEq(_storedHash(_h(c, "collect2")), _h(c, "collect2"));
        assertEq(vault.roundKeyForCollect(_batch(256)), _h(c, "collect256"));
        assertEq(_storedHash(_h(c, "collect256")), _h(c, "collect256"));
        assertEq(vault.roundKeyForPayLot(), _h(c, "payLotKey"));
        assertEq(_storedHash(_h(c, "payLotKey")), _h(c, "payLot"));
        assertEq(vault.roundKeyForAdd(CANDIDATE), _h(c, "addKey"));
        assertEq(_storedHash(_h(c, "addKey")), _h(c, "add"));
        assertEq(vault.roundKeyForRemove(V3), _h(c, "removeKey"));
        assertEq(_storedHash(_h(c, "removeKey")), _h(c, "remove"));
        assertEq(vault.roundKeyForConfig(cfg, set), _h(c, "config"));
        assertEq(_storedHash(_h(c, "config")), _h(c, "config"));
    }
}

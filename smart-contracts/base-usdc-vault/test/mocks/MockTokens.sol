// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC20Permit} from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Permit.sol";
import {ReentrancyGuardTransient} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import {AllwaysUsdcVault} from "../../src/AllwaysUsdcVault.sol";

/// FiatToken-like: 6 decimals, EIP-2612, issuer blacklist and pause, and a balance wipe that
/// stands in for any issuer action on a holder.
contract MockFiatToken is ERC20Permit {
    mapping(address => bool) public isBlacklisted;
    bool public paused;

    constructor() ERC20("USD Coin", "USDC") ERC20Permit("USD Coin") {}

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }

    function blacklist(address a) external {
        isBlacklisted[a] = true;
    }

    function unBlacklist(address a) external {
        isBlacklisted[a] = false;
    }

    function pause() external {
        paused = true;
    }

    function unpause() external {
        paused = false;
    }

    function wipe(address holder, uint256 amount) external {
        _burn(holder, amount);
    }

    function transferFrom(address from, address to, uint256 value) public override returns (bool) {
        require(!isBlacklisted[msg.sender], "Blacklistable: account is blacklisted");
        return super.transferFrom(from, to, value);
    }

    function permit(
        address owner,
        address spender,
        uint256 value,
        uint256 deadline,
        uint8 v,
        bytes32 r,
        bytes32 s
    ) public override {
        require(!paused, "Pausable: paused");
        super.permit(owner, spender, value, deadline, v, r, s);
    }

    function _update(address from, address to, uint256 value) internal override {
        // Mint and wipe are issuer actions and bypass the checks, as on FiatToken.
        if (from != address(0) && to != address(0)) {
            require(!paused, "Pausable: paused");
            require(!isBlacklisted[from] && !isBlacklisted[to], "Blacklistable: account is blacklisted");
        }
        super._update(from, to, value);
    }
}

contract SixDecimalToken is ERC20 {
    constructor() ERC20("Mock", "MOCK") {}

    function decimals() public pure virtual override returns (uint8) {
        return 6;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

contract EighteenDecimalToken is SixDecimalToken {
    function decimals() public pure override returns (uint8) {
        return 18;
    }
}

/// Burns 1% of every transfer, so the vault receives less than it pulled.
contract FeeOnTransferToken is SixDecimalToken {
    function _update(address from, address to, uint256 value) internal override {
        if (from != address(0) && to != address(0)) {
            uint256 fee = value / 100;
            super._update(from, address(0), fee);
            value -= fee;
        }
        super._update(from, to, value);
    }
}

/// `transfer` spins until it runs out of gas once armed.
contract GasBurnerToken is SixDecimalToken {
    bool public armed;

    function arm(bool on) external {
        armed = on;
    }

    function transfer(address to, uint256 value) public override returns (bool) {
        if (armed) {
            while (true) {}
        }
        return super.transfer(to, value);
    }
}

/// Answers `transfer` with a revert bomb (200 KB) or a "success" bomb (100 KB, first word 1).
contract ReturnBombToken is SixDecimalToken {
    uint8 public mode; // 0 normal, 1 revert bomb, 2 success bomb

    function setMode(uint8 m) external {
        mode = m;
    }

    function transfer(address to, uint256 value) public override returns (bool) {
        uint8 m = mode;
        if (m == 0) return super.transfer(to, value);
        if (m == 2) super.transfer(to, value);
        assembly {
            mstore(0, 1)
            if eq(m, 1) { revert(0, 200000) }
            return(0, 100000)
        }
    }
}

/// Returns no data (old-style ERC20), or a malformed / non-boolean word, depending on mode.
contract OddReturnToken is SixDecimalToken {
    uint8 public mode; // 0 normal, 1 empty, 2 short (1 byte), 3 word = 2

    function setMode(uint8 m) external {
        mode = m;
    }

    function transfer(address to, uint256 value) public override returns (bool) {
        uint8 m = mode;
        super.transfer(to, value);
        if (m == 0) return true;
        assembly {
            if eq(m, 1) { return(0, 0) }
            mstore(0, 2)
            if eq(m, 2) { return(31, 1) }
            return(0, 32)
        }
    }
}

/// Re-enters every vault entry point from inside transfer/transferFrom and records the outcome.
contract ReentrantToken is SixDecimalToken {
    AllwaysUsdcVault public vault;
    bool public armed;
    uint256 public attempts;
    uint256 public blockedByGuard;
    bool public anySucceeded;

    function setVault(AllwaysUsdcVault v) external {
        vault = v;
    }

    function arm(bool on) external {
        armed = on;
    }

    function _update(address from, address to, uint256 value) internal override {
        if (armed && from != address(0) && to != address(0)) {
            armed = false;
            _reenterAll();
            armed = true;
        }
        super._update(from, to, value);
    }

    function _reenterAll() private {
        AllwaysUsdcVault.FeeEntry[] memory entries = new AllwaysUsdcVault.FeeEntry[](1);
        entries[0] = AllwaysUsdcVault.FeeEntry(address(this), 1);
        AllwaysUsdcVault.Config memory cfg = vault.getConfig();
        bytes[15] memory calls = [
            abi.encodeCall(AllwaysUsdcVault.postCollateral, (1)),
            abi.encodeCall(AllwaysUsdcVault.postCollateralWithPermit, (1, 0, 0, 0, 0)),
            abi.encodeCall(AllwaysUsdcVault.withdrawCollateral, (1)),
            abi.encodeCall(AllwaysUsdcVault.lockBond, (bytes32(uint256(1)))),
            abi.encodeCall(AllwaysUsdcVault.selfUnlock, ()),
            abi.encodeCall(AllwaysUsdcVault.ping, ()),
            abi.encodeCall(AllwaysUsdcVault.voteUnlock, (address(this), 1)),
            abi.encodeCall(
                AllwaysUsdcVault.voteSlash, (address(this), bytes32(uint256(1)), 1, address(this), 1)
            ),
            abi.encodeCall(AllwaysUsdcVault.claimSlash, (bytes32(uint256(1)))),
            abi.encodeCall(AllwaysUsdcVault.voteCollectFeesBatch, (entries)),
            abi.encodeCall(AllwaysUsdcVault.votePayLot, (bytes32(uint256(1)), address(this), 1, 1)),
            abi.encodeCall(AllwaysUsdcVault.voteAddValidator, (address(this))),
            abi.encodeCall(AllwaysUsdcVault.acceptValidator, ()),
            abi.encodeCall(AllwaysUsdcVault.voteRemoveValidator, (address(this))),
            abi.encodeCall(AllwaysUsdcVault.voteSetConfig, (cfg))
        ];
        for (uint256 i; i < calls.length; ++i) {
            ++attempts;
            (bool ok, bytes memory ret) = address(vault).call(calls[i]);
            if (ok) {
                anySucceeded = true;
            } else if (
                ret.length >= 4
                    && bytes4(ret) == ReentrancyGuardTransient.ReentrancyGuardReentrantCall.selector
            ) {
                ++blockedByGuard;
            }
        }
    }
}

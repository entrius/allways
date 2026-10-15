// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {Test} from "forge-std/Test.sol";
import {Vm} from "forge-std/Vm.sol";
import {AllwaysUsdcVault} from "../../src/AllwaysUsdcVault.sol";
import {MockFiatToken} from "../mocks/MockTokens.sol";

/// Drives every vault entry point as random miners, validators, users and donors, while the token
/// randomly blacklists and pauses. Ghost state records anything an invariant forbids.
contract VaultHandler is Test {
    AllwaysUsdcVault public vault;
    MockFiatToken public usdc;

    uint256 internal constant N_MINERS = 4;
    uint256 internal constant N_REFS = 24;

    address[] public miners;
    uint256[] internal minerKeys;
    address[] public pool; // validator pool: the live set is drawn from it
    address[] public users;
    bytes32[] public refs;
    bytes32[] public lotRefs;

    // ghosts
    bool public illegalBondMove;
    bool public illegalLockMove;
    bool public epochRegressed;
    bool public markerViolation;
    bool public potOutflowViolation;
    bool public settledRegressed;
    bool public unexplainedBalanceMove;
    mapping(bytes32 => uint256) public slashApplications;
    mapping(bytes32 => uint256) public lotSettlements;
    uint256 public expectedBalance;
    // Most votes target the "current" ref/lot/candidate so rounds actually reach quorum.
    uint256 public slashCursor;
    uint256 public lotCursor;
    uint256 public candCursor;
    mapping(uint256 => uint64) internal lotDeadline;
    uint256 public ghostLotsPaid;

    // call stats (reported, not asserted)
    mapping(bytes32 => uint256) public calls;
    uint256 public slashQuorums;
    uint256 public lotsPaid;
    uint256 public lotsFailed;
    uint256 public unlockQuorums;
    uint256 public selfUnlocks;
    uint256 public parkedSlashes;
    uint256 public membershipChanges;
    uint256 public configChanges;

    bytes32 internal constant MINER_SLASHED =
        keccak256("MinerSlashed(address,bytes32,uint256,uint256,uint256)");
    bytes32 internal constant LOT_PAID = keccak256("LotPaid(bytes32,address,uint256,uint256)");
    bytes32 internal constant LOT_FAILED = keccak256("LotPayFailed(bytes32,address,uint256)");

    struct Snap {
        uint256[N_MINERS] collateral;
        bool[N_MINERS] locked;
        uint64[N_MINERS] epoch;
        uint256[N_MINERS] settled;
        uint256 balance;
    }

    constructor(AllwaysUsdcVault vault_, MockFiatToken usdc_, address[] memory pool_) {
        vault = vault_;
        usdc = usdc_;
        pool = pool_;
        for (uint256 i; i < N_MINERS; ++i) {
            (address m, uint256 k) = makeAddrAndKey(string.concat("miner", vm.toString(i)));
            miners.push(m);
            minerKeys.push(k);
        }
        users.push(makeAddr("user0"));
        users.push(makeAddr("user1"));
        users.push(address(vault_)); // a ghosted system swap reimburses the vault itself
        for (uint256 i; i < N_REFS; ++i) {
            refs.push(keccak256(abi.encode("swap", i)));
            lotRefs.push(keccak256(abi.encode("lot", i)));
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    function _miner(uint256 s) internal view returns (uint256, address) {
        uint256 i = s % N_MINERS;
        return (i, miners[i]);
    }

    function _validator(uint256 s) internal view returns (address) {
        address[] memory set = vault.getValidators();
        return set[s % set.length];
    }

    /// First miner from `s` whose bond is locked and at the floor, else `s` itself.
    function _bondedMiner(uint256 s) internal view returns (uint256) {
        for (uint256 k; k < N_MINERS; ++k) {
            (uint256 c, bool l,,) = vault.getBond(miners[(s + k) % N_MINERS]);
            if (l && c >= vault.minCollateral()) return s + k;
        }
        return s;
    }

    function _snap() internal view returns (Snap memory sn) {
        for (uint256 i; i < N_MINERS; ++i) {
            (uint256 c, bool l, uint64 e,) = vault.getBond(miners[i]);
            sn.collateral[i] = c;
            sn.locked[i] = l;
            sn.epoch[i] = e;
            sn.settled[i] = vault.settledTotal(miners[i]);
        }
        sn.balance = usdc.balanceOf(address(vault));
    }

    /// Bonds may fall only by the miner's own withdraw (while unlocked), a slash quorum or a fee
    /// quorum; locks may open only by an unlock quorum or a self-unlock. `withdrawer`, `unlocker`
    /// name the one miner an action may legitimately touch (type(uint256).max = none).
    function _check(Snap memory b, bool slashOrFee, uint256 withdrawer, uint256 unlocker, bool unlockAll)
        internal
    {
        Snap memory a = _snap();
        for (uint256 i; i < N_MINERS; ++i) {
            if (a.collateral[i] < b.collateral[i]) {
                bool ok = slashOrFee || (i == withdrawer && !b.locked[i]);
                if (!ok) illegalBondMove = true;
            }
            if (a.epoch[i] < b.epoch[i]) epochRegressed = true;
            if (a.locked[i] != b.locked[i] && a.epoch[i] != b.epoch[i] + 1) epochRegressed = true;
            if (b.locked[i] && !a.locked[i] && !(unlockAll || i == unlocker)) illegalLockMove = true;
            if (a.settled[i] < b.settled[i]) settledRegressed = true;
        }
    }

    /// MinerSlashed events for `ref` (topics: signature, miner, swapRef).
    function _countSlashes(bytes32 ref) internal returns (uint256 n) {
        Vm.Log[] memory logs = vm.getRecordedLogs();
        for (uint256 i; i < logs.length; ++i) {
            if (
                logs[i].emitter == address(vault) && logs[i].topics[0] == MINER_SLASHED
                    && logs[i].topics[2] == ref
            ) {
                ++n;
            }
        }
    }

    function _settle(uint256 balanceBefore, uint256 expectedOut, uint256 expectedIn) internal {
        uint256 bal = usdc.balanceOf(address(vault));
        if (bal + expectedOut != balanceBefore + expectedIn) unexplainedBalanceMove = true;
        expectedBalance = expectedBalance + expectedIn - expectedOut;
    }

    // ── miner actions ───────────────────────────────────────────────────────

    function post(uint256 ms, uint256 amount) external {
        calls["post"]++;
        (uint256 i, address m) = _miner(ms);
        amount = bound(amount, 1, 30_000e6);
        Snap memory b = _snap();
        usdc.mint(m, amount);
        vm.startPrank(m);
        usdc.approve(address(vault), amount);
        bool ok;
        try vault.postCollateral(amount) {
            ok = true;
        } catch {}
        vm.stopPrank();
        _check(b, false, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, 0, ok ? amount : 0);
        if (ok && _snap().collateral[i] != b.collateral[i] + amount) illegalBondMove = true;
    }

    function postWithPermit(uint256 ms, uint256 amount) external {
        calls["postWithPermit"]++;
        (uint256 i, address m) = _miner(ms);
        amount = bound(amount, 1, 30_000e6);
        Snap memory b = _snap();
        usdc.mint(m, amount);
        uint256 deadline = block.timestamp + 1 hours;
        bytes32 structHash = keccak256(
            abi.encode(
                keccak256(
                    "Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)"
                ),
                m,
                address(vault),
                amount,
                usdc.nonces(m),
                deadline
            )
        );
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(
            minerKeys[i], keccak256(abi.encodePacked("\x19\x01", usdc.DOMAIN_SEPARATOR(), structHash))
        );
        vm.prank(m);
        bool ok;
        try vault.postCollateralWithPermit(amount, deadline, v, r, s) {
            ok = true;
        } catch {}
        _check(b, false, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, 0, ok ? amount : 0);
    }

    function withdraw(uint256 ms, uint256 amount) external {
        calls["withdraw"]++;
        (uint256 i, address m) = _miner(ms);
        Snap memory b = _snap();
        amount = bound(amount, 1, b.collateral[i] + 1);
        vm.prank(m);
        bool ok;
        try vault.withdrawCollateral(amount) {
            ok = true;
        } catch {}
        _check(b, false, i, type(uint256).max, false);
        _settle(b.balance, ok ? amount : 0, 0);
    }

    function lock(uint256 ms, bytes32 pk) external {
        calls["lock"]++;
        (, address m) = _miner(ms);
        Snap memory b = _snap();
        vm.prank(m);
        try vault.lockBond(pk == bytes32(0) ? bytes32(uint256(1)) : pk) {} catch {}
        _check(b, false, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, 0, 0);
    }

    function selfUnlock(uint256 ms) external {
        calls["selfUnlock"]++;
        (uint256 i, address m) = _miner(ms);
        Snap memory b = _snap();
        vm.prank(m);
        try vault.selfUnlock() {
            selfUnlocks++;
        } catch {}
        _check(b, false, type(uint256).max, i, false);
        _settle(b.balance, 0, 0);
    }

    function claimSlash(uint256 rs, uint256 callerSeed) external {
        calls["claimSlash"]++;
        // Usually claim a parked reimbursement if there is one; sometimes a random ref.
        bytes32 ref = refs[rs % N_REFS];
        if (rs % 3 != 0) {
            for (uint256 k; k < N_REFS; ++k) {
                (address u,) = vault.pendingSlash(refs[k]);
                if (u != address(0)) {
                    ref = refs[k];
                    break;
                }
            }
        }
        (, uint128 amt) = vault.pendingSlash(ref);
        Snap memory b = _snap();
        vm.prank(users[callerSeed % 2]);
        bool ok;
        try vault.claimSlash(ref) {
            ok = true;
        } catch {}
        _check(b, false, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, ok ? amt : 0, 0);
    }

    function donate(uint256 amount) external {
        calls["donate"]++;
        amount = bound(amount, 1, 5000e6);
        Snap memory b = _snap();
        usdc.mint(address(this), amount);
        bool ok;
        try usdc.transfer(address(vault), amount) {
            ok = true;
        } catch {}
        _settle(b.balance, 0, ok ? amount : 0);
    }

    // ── validator actions ───────────────────────────────────────────────────

    function voteUnlock(uint256 vs, uint256 ms, bool stale) external {
        calls["voteUnlock"]++;
        (uint256 i, address m) = _miner(ms);
        Snap memory b = _snap();
        vm.prank(_validator(vs));
        try vault.voteUnlock(m, stale ? b.epoch[i] + 1 : b.epoch[i]) {} catch {}
        if (b.locked[i] && !_snap().locked[i]) unlockQuorums++;
        _check(b, false, type(uint256).max, i, false);
        _settle(b.balance, 0, 0);
    }

    struct SlashCall {
        uint256 idx;
        uint256 mi;
        address m;
        bytes32 ref;
        uint256 penalty;
        uint256 reimb;
        address payee;
        uint256 pendingBefore;
    }

    function voteSlash(uint256 vs, uint256 ms, uint256 rs, uint256 verdict) external {
        calls["voteSlash"]++;
        SlashCall memory c;
        for (uint256 k; k < N_REFS && vault.isSlashed(refs[slashCursor % N_REFS]); ++k) {
            slashCursor++;
        }
        c.idx = rs % 5 == 0 ? rs % N_REFS : slashCursor % N_REFS;
        c.ref = refs[c.idx];
        (c.mi, c.m) = _miner(rs % 5 == 0 ? ms : c.idx);
        // The verdict follows the ref; one vote in six relays a divergent one.
        uint256 vk = verdict % 6 == 0 ? c.idx + 1 : c.idx;
        c.penalty = vk % 2 == 0 ? 100e6 : 3000e6;
        c.reimb = vk % 3 == 0 ? 0 : vk % 3 == 1 ? c.penalty / 2 : c.penalty;
        c.payee = users[(vk / 3) % users.length];

        Snap memory b = _snap();
        c.pendingBefore = vault.pendingSlashTotal();
        vm.recordLogs();
        vm.prank(_validator(vs));
        try vault.voteSlash(c.m, c.ref, c.penalty, c.payee, c.reimb) {} catch {}
        uint256 applied = _countSlashes(c.ref);
        if (applied > 0) {
            slashQuorums++;
            if (c.idx == slashCursor % N_REFS) slashCursor++;
            slashApplications[c.ref] += applied;
            if (slashApplications[c.ref] > 1) markerViolation = true;
        }
        _check(b, applied > 0, type(uint256).max, type(uint256).max, false);
        // Outflow = the reimbursement actually pushed (not parked, not kept as pot for the vault).
        uint256 pushed;
        if (applied > 0 && c.payee != address(vault)) {
            uint256 seized = c.penalty < b.collateral[c.mi] ? c.penalty : b.collateral[c.mi];
            pushed = c.reimb < seized ? c.reimb : seized;
            uint256 parked = vault.pendingSlashTotal() - c.pendingBefore;
            if (parked > 0) parkedSlashes++;
            pushed -= parked;
        }
        _settle(b.balance, pushed, 0);
    }

    function voteCollect(uint256 vs, uint256 rs) external {
        calls["voteCollect"]++;
        // Every miner, ascending, at one of a few cumulative levels.
        address[] memory sorted = new address[](N_MINERS);
        for (uint256 i; i < N_MINERS; ++i) {
            sorted[i] = miners[i];
        }
        for (uint256 i; i < N_MINERS; ++i) {
            for (uint256 j = i + 1; j < N_MINERS; ++j) {
                if (sorted[j] < sorted[i]) (sorted[i], sorted[j]) = (sorted[j], sorted[i]);
            }
        }
        uint256 level = rs % 4;
        AllwaysUsdcVault.FeeEntry[] memory e = new AllwaysUsdcVault.FeeEntry[](N_MINERS);
        for (uint256 i; i < N_MINERS; ++i) {
            e[i] = AllwaysUsdcVault.FeeEntry(sorted[i], level * 40e6 * (i + 1));
        }
        Snap memory b = _snap();
        vm.prank(_validator(vs));
        try vault.voteCollectFeesBatch(e) {} catch {}
        _check(b, true, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, 0, 0);
    }

    struct LotCall {
        bytes32 lotRef;
        uint256 i;
        address m;
        uint256 amount;
        uint64 deadline;
        uint256 potBefore;
        uint256 minerBal;
        AllwaysUsdcVault.LotStatus statusBefore;
    }

    function votePayLot(uint256 vs, uint256 ls, uint256 ms, uint256 as_, uint256 ds) external {
        calls["votePayLot"]++;
        LotCall memory c;
        for (
            uint256 k;
            k < N_REFS && vault.lotStatus(lotRefs[lotCursor % N_REFS]) != AllwaysUsdcVault.LotStatus.None;
            ++k
        ) {
            lotCursor++;
        }
        uint256 idx = ls % 5 == 0 ? ls % N_REFS : lotCursor % N_REFS;
        c.lotRef = lotRefs[idx];
        (c.i, c.m) = _miner(ls % 5 == 0 ? ms : _bondedMiner(idx));
        uint256[4] memory amounts = [uint256(1e6), 50e6, 500e6, 2000e6];
        uint64[4] memory windows = [uint64(60), 1800, 3600, 4000];
        c.amount = amounts[(ls % 5 == 0 ? as_ : idx) % 4];
        // A lot keeps one absolute deadline until it passes, so votes agree on the tuple.
        if (lotDeadline[idx] < block.timestamp) lotDeadline[idx] = uint64(block.timestamp) + windows[ds % 4];
        c.deadline = lotDeadline[idx];
        Snap memory b = _snap();
        c.potBefore = vault.pot();
        c.minerBal = usdc.balanceOf(c.m);
        c.statusBefore = vault.lotStatus(c.lotRef);
        vm.recordLogs();
        vm.prank(_validator(vs));
        try vault.votePayLot(c.lotRef, c.m, c.amount, c.deadline) {} catch {}
        (uint256 paid, uint256 settled) = _lotLogs(c.lotRef);
        if (c.statusBefore != AllwaysUsdcVault.LotStatus.None && vault.lotStatus(c.lotRef) != c.statusBefore)
        {
            markerViolation = true;
        }
        if (vault.lotStatus(c.lotRef) != AllwaysUsdcVault.LotStatus.None && idx == lotCursor % N_REFS) {
            lotCursor++;
        }
        if (settled > 0) {
            lotSettlements[c.lotRef] += settled;
            if (lotSettlements[c.lotRef] > 1) markerViolation = true;
        }
        if (paid > 0) {
            lotsPaid++;
            ghostLotsPaid += c.amount;
            bool covered = b.collateral[c.i] * 10_000 >= c.amount * 11_000;
            if (
                c.amount > c.potBefore || c.amount > vault.maxLotAmount() || !b.locked[c.i] || !covered
                    || b.collateral[c.i] < vault.minCollateral()
                    || usdc.balanceOf(c.m) != c.minerBal + c.amount
            ) potOutflowViolation = true;
        }
        _check(b, false, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, paid > 0 ? c.amount : 0, 0);
    }

    function _lotLogs(bytes32 lotRef) internal returns (uint256 paid, uint256 settled) {
        Vm.Log[] memory logs = vm.getRecordedLogs();
        for (uint256 k; k < logs.length; ++k) {
            if (logs[k].emitter != address(vault) || logs[k].topics[1] != lotRef) continue;
            if (logs[k].topics[0] == LOT_PAID) {
                ++paid;
                ++settled;
            } else if (logs[k].topics[0] == LOT_FAILED) {
                ++settled;
                lotsFailed++;
            }
        }
    }

    function voteAdd(uint256 vs, uint256 cs) external {
        calls["voteAdd"]++;
        Snap memory b = _snap();
        address cand = pool[(cs % 4 == 0 ? cs : candCursor) % pool.length];
        if (vault.isValidator(cand)) candCursor++;
        address voter = _validator(vs);
        vm.prank(voter);
        try vault.voteAddValidator(cand) {} catch {}
        _check(b, false, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, 0, 0);
    }

    function accept(uint256 cs) external {
        calls["accept"]++;
        Snap memory b = _snap();
        vm.prank(pool[(cs % 4 == 0 ? cs : candCursor) % pool.length]);
        try vault.acceptValidator() {
            membershipChanges++;
        } catch {}
        _check(b, false, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, 0, 0);
    }

    function voteRemove(uint256 vs, uint256 ts) external {
        calls["voteRemove"]++;
        Snap memory b = _snap();
        uint256 n = vault.getValidators().length;
        address voter = _validator(vs);
        address target = _validator(ts);
        vm.prank(voter);
        try vault.voteRemoveValidator(target) {} catch {}
        if (vault.getValidators().length < n) membershipChanges++;
        _check(b, false, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, 0, 0);
    }

    function voteConfig(uint256 vs, uint256 cs) external {
        calls["voteConfig"]++;
        AllwaysUsdcVault.Config[3] memory cfgs = [
            AllwaysUsdcVault.Config(10e6, 100_000e6, 66, 1800, 1000e6),
            AllwaysUsdcVault.Config(1e6, 0, 51, 600, 1e6),
            AllwaysUsdcVault.Config(500e6, 20_000e6, 100, 86_400, 1e10)
        ];
        AllwaysUsdcVault.Config memory c = cfgs[cs % 3];
        Snap memory b = _snap();
        vm.prank(_validator(vs));
        try vault.voteSetConfig(c) {
            if (vault.consensusThresholdPct() == c.consensusThresholdPct) configChanges++;
        } catch {}
        _check(b, false, type(uint256).max, type(uint256).max, false);
        _settle(b.balance, 0, 0);
    }

    function ping(uint256 vs) external {
        calls["ping"]++;
        vm.prank(_validator(vs));
        vault.ping();
    }

    // ── environment ─────────────────────────────────────────────────────────

    /// Issuer blacklists and pauses; skewed toward lifting them so the vault spends most of its time live.
    function issuerAction(uint256 s) external {
        calls["issuer"]++;
        uint256 kind = s % 10;
        address target = (s / 10) % 4 == 0 ? address(vault) : _anyActor(s / 40);
        if (kind < 2) usdc.blacklist(target);
        else if (kind < 6) usdc.unBlacklist(target);
        else if (kind == 6) usdc.pause();
        else usdc.unpause();
    }

    function _anyActor(uint256 s) internal view returns (address) {
        uint256 k = s % (N_MINERS + 2);
        return k < N_MINERS ? miners[k] : users[k - N_MINERS];
    }

    function warp(uint256 s) external {
        calls["warp"]++;
        uint256 dt = s % 20 == 0 ? 15 days : s % 5 == 0 ? bound(s, 1, 2 days) : bound(s, 1, 300);
        skip(dt);
    }
}

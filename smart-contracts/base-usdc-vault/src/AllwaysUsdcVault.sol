// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {IERC20Permit} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Permit.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {ReentrancyGuardTransient} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";

/// @title AllwaysUsdcVault
/// @notice Holds miner bonds in USDC on Base and applies validator-quorum verdicts (unlock, slash,
/// fee settle, lot payment). There is no owner, admin, pause, proxy or sweep: the validator set and
/// config change only by unanimous validator vote, inside compile-time bounds.
contract AllwaysUsdcVault is ReentrancyGuardTransient {
    using SafeERC20 for IERC20;

    // ─── constants ──────────────────────────────────────────────────────────

    uint256 public constant VERSION = 1;

    uint256 public constant MAX_VALIDATORS = 16;
    uint256 public constant MIN_VALIDATORS_TO_REMOVE = 3;
    // ceil(n*51/100) is a strict majority at every n, so no minority quorum can fabricate a slash.
    uint8 public constant MIN_THRESHOLD = 51;
    uint8 public constant MAX_THRESHOLD = 100;
    // Floor stops a unanimous mistake from making rounds unfinishable; ceiling stops sleeper rounds.
    uint32 public constant MIN_VOTE_ROUND_TTL = 600;
    uint32 public constant MAX_VOTE_ROUND_TTL = 86_400;
    // Not governable: a captured quorum could otherwise set it to zero or forever.
    uint64 public constant DEAD_MAN_DELAY = 14 days;
    uint256 public constant MAX_BATCH = 256;

    uint128 public constant MIN_COLLATERAL_FLOOR = 1e6;
    uint128 public constant MIN_COLLATERAL_CEIL = 1e12;
    uint128 public constant MAX_COLLATERAL_CEIL = 1e13;
    uint128 public constant MIN_LOT_AMOUNT = 1e6;
    uint128 public constant HARD_MAX_LOT_AMOUNT = 1e10;
    uint64 public constant MAX_LOT_DEADLINE_WINDOW = 3600;
    uint256 public constant LOT_COVERAGE_BPS = 11_000;

    // Gas forwarded to every push transfer. The caller must hold enough to forward all of it, so a
    // push fails only when the token refuses, never because the voter sent a tight gas limit.
    uint256 public constant PUSH_GAS = 120_000;
    uint256 private constant PUSH_GAS_RESERVE = PUSH_GAS + PUSH_GAS / 63 + 5000;

    uint8 public constant REQ_UNLOCK = 0;
    uint8 public constant REQ_SLASH = 1;
    uint8 public constant REQ_COLLECT = 2;
    uint8 public constant REQ_ADD_VALIDATOR = 3;
    uint8 public constant REQ_REMOVE_VALIDATOR = 4;
    uint8 public constant REQ_CONFIG = 5;
    // 6 belonged to the TAO vault's recycle target and is never reused.
    uint8 public constant REQ_PAY_LOT = 7;

    bytes32 private constant LOT_ROUND_KEY = keccak256(abi.encode(REQ_PAY_LOT));

    // ─── types ──────────────────────────────────────────────────────────────

    struct Config {
        uint128 minCollateral;
        uint128 maxCollateral;
        uint8 consensusThresholdPct;
        uint32 voteRoundTtl;
        uint128 maxLotAmount;
    }

    struct Pending {
        address candidate;
        uint64 approvedAt;
        bytes32 setHash;
    }

    struct Bond {
        uint128 collateral;
        uint64 epoch;
        bool locked;
    }

    struct Round {
        bytes32 hash;
        uint64 id;
        uint64 createdAt;
        address[] voters;
    }

    struct PendingSlash {
        address user;
        uint128 amount;
    }

    struct FeeEntry {
        address miner;
        uint256 cumulative;
    }

    enum LotStatus {
        None,
        Paid,
        Failed
    }

    struct OpenLot {
        bytes32 lotRef;
        uint64 deadline;
    }

    // ─── storage ────────────────────────────────────────────────────────────

    IERC20 public immutable usdc;

    Config internal config;

    address[] internal validators;
    mapping(address => bool) public isValidator;
    Pending[] internal pendingValidators;

    mapping(address => Bond) internal bonds;
    mapping(address => bytes32) public solanaPubkeyOf;

    mapping(bytes32 => Round) internal rounds;
    uint64 public nextRoundId = 1;
    uint64 public lastValidatorActivity;

    mapping(address => uint256) public settledTotal;
    mapping(bytes32 => bool) public isSlashed;
    mapping(bytes32 => PendingSlash) public pendingSlash;

    uint256 public totalCollateral;
    uint256 public pendingSlashTotal;
    uint256 public accumulatedFees;
    uint256 public totalLotsPaid;

    mapping(bytes32 => LotStatus) public lotStatus;
    OpenLot public openLot;

    // ─── events ─────────────────────────────────────────────────────────────

    event CollateralPosted(address indexed miner, uint256 amount, uint256 total);
    event CollateralWithdrawn(address indexed miner, uint256 amount, uint256 remaining);
    event BondLockChanged(address indexed miner, bool locked, uint64 epoch, bytes32 solanaPubkey);
    event VaultVoteCast(
        address indexed validator, uint8 reqType, uint64 roundId, bytes32 indexed subject, uint256 voteCount
    );
    event MinerSlashed(
        address indexed miner, bytes32 indexed swapRef, uint256 seized, uint256 reimbursed, uint256 surplus
    );
    event SlashPending(bytes32 indexed swapRef, address indexed user, uint256 amount);
    event SlashClaimed(bytes32 indexed swapRef, address indexed user, uint256 amount);
    event FeesSettled(address indexed miner, uint256 collected, uint256 newCumulative, uint256 shortfall);
    event LotOpened(
        bytes32 indexed lotRef, address indexed miner, uint256 amount, uint64 deadline, address indexed opener
    );
    event LotPaid(bytes32 indexed lotRef, address indexed miner, uint256 amount, uint256 potAfter);
    event LotPayFailed(bytes32 indexed lotRef, address indexed miner, uint256 amount);
    event ValidatorPending(address indexed candidate, uint64 expiresAt);
    event ValidatorUpdated(address indexed validator, bool registered);
    event ConfigUpdated(Config cfg);

    // ─── errors ─────────────────────────────────────────────────────────────

    error NotValidator();
    error InvalidValidatorSet();
    error AlreadyValidator();
    error NotPendingValidator();
    error AdmissionVoid();
    error SelfRemoval();
    error AlreadyVoted();
    error PendingConflict();
    error InvalidAmount();
    error InsufficientCollateral();
    error ExceedsMaxCollateral();
    error BondLocked();
    error LockStateUnchanged();
    error EpochMismatch();
    error AlreadySlashed();
    error InvalidReimbursement();
    error NoPendingSlash();
    error InvalidBatch();
    error ConfigOutOfBounds(uint8 field);
    error ZeroAddress();
    error InvalidToken();
    error InvalidPayee();
    error BatchNotSorted();
    error DepositMismatch(uint256 expected, uint256 received);
    error InvalidSolanaPubkey();
    error InvalidLotRef();
    error LotAlreadyOpen(bytes32 openLotRef);
    error LotAlreadySettled();
    error LotDeadlinePassed();
    error LotDeadlineTooFar();
    error LotExceedsPot(uint256 amount, uint256 pot);
    error LotExceedsCap();
    error RecipientNotBonded();
    error RecipientUnderCovered();
    error DeadManNotElapsed(uint64 availableAt);
    error InsufficientGas();

    // ─── modifiers ──────────────────────────────────────────────────────────

    /// Validator-only, and every successful validator call restarts the dead-man clock. A revert
    /// rolls the timestamp back with everything else.
    modifier onlyValidator() {
        if (!isValidator[msg.sender]) revert NotValidator();
        lastValidatorActivity = uint64(block.timestamp);
        _;
    }

    // ─── constructor ────────────────────────────────────────────────────────

    /// @notice Seeds the only validator set the vault will ever start from. A bad seed fails the
    /// deploy: a bricked immutable vault is worse than a failed one. The deployer keeps no rights.
    constructor(IERC20 usdc_, address[] memory validators_, Config memory cfg) {
        if (address(usdc_).code.length == 0) revert InvalidToken();
        if (IERC20Metadata(address(usdc_)).decimals() != 6) revert InvalidToken();
        uint256 n = validators_.length;
        if (n == 0 || n > MAX_VALIDATORS) revert InvalidValidatorSet();
        for (uint256 i; i < n; ++i) {
            address v = validators_[i];
            if (v == address(0) || v == address(this) || isValidator[v]) revert InvalidValidatorSet();
            isValidator[v] = true;
            validators.push(v);
            emit ValidatorUpdated(v, true);
        }
        _checkConfig(cfg);
        usdc = usdc_;
        config = cfg;
        lastValidatorActivity = uint64(block.timestamp);
        emit ConfigUpdated(cfg);
    }

    // ─── bonds ──────────────────────────────────────────────────────────────

    /// @notice Post `amount` USDC (pre-approved) into the caller's bond. Allowed while locked.
    function postCollateral(uint256 amount) external nonReentrant {
        uint256 total = _creditBond(amount);
        _pull(amount);
        emit CollateralPosted(msg.sender, amount, total);
    }

    /// @notice As `postCollateral`, with an EIP-2612 permit in the same tx. A front-run permit is
    /// ignored: the allowance it set is already there.
    function postCollateralWithPermit(uint256 amount, uint256 deadline, uint8 v, bytes32 r, bytes32 s)
        external
        nonReentrant
    {
        uint256 total = _creditBond(amount);
        try IERC20Permit(address(usdc)).permit(msg.sender, address(this), amount, deadline, v, r, s) {}
            catch {}
        _pull(amount);
        emit CollateralPosted(msg.sender, amount, total);
    }

    /// @notice Withdraw to the caller only, and only while unlocked. There is deliberately no `to`.
    function withdrawCollateral(uint256 amount) external nonReentrant {
        if (amount == 0) revert InvalidAmount();
        Bond storage b = bonds[msg.sender];
        if (b.locked) revert BondLocked();
        uint256 current = b.collateral;
        if (amount > current) revert InsufficientCollateral();
        uint256 remaining = current - amount;
        b.collateral = uint128(remaining);
        totalCollateral -= amount;
        usdc.safeTransfer(msg.sender, amount);
        emit CollateralWithdrawn(msg.sender, amount, remaining);
    }

    /// @notice Lock the caller's bond and name the Solana miner it backs. The pubkey is a claim;
    /// validators attest only when the Solana side names this address back.
    function lockBond(bytes32 solanaPubkey) external nonReentrant {
        Bond storage b = bonds[msg.sender];
        if (b.locked) revert LockStateUnchanged();
        if (b.collateral < config.minCollateral) revert InsufficientCollateral();
        if (solanaPubkey == bytes32(0)) revert InvalidSolanaPubkey();
        uint64 epoch = b.epoch + 1;
        b.locked = true;
        b.epoch = epoch;
        solanaPubkeyOf[msg.sender] = solanaPubkey;
        emit BondLockChanged(msg.sender, true, epoch, solanaPubkey);
    }

    /// @notice Unlock the caller's own bond once no validator has transacted for DEAD_MAN_DELAY.
    /// Moves no funds; the epoch bump makes any old attestation stale.
    function selfUnlock() external nonReentrant {
        Bond storage b = bonds[msg.sender];
        if (!b.locked) revert LockStateUnchanged();
        uint64 availableAt = lastValidatorActivity + DEAD_MAN_DELAY + 1;
        if (block.timestamp < availableAt) revert DeadManNotElapsed(availableAt);
        uint64 epoch = b.epoch + 1;
        b.locked = false;
        b.epoch = epoch;
        emit BondLockChanged(msg.sender, false, epoch, solanaPubkeyOf[msg.sender]);
    }

    /// @notice Validator liveness signal; does nothing but restart the dead-man clock.
    function ping() external nonReentrant onlyValidator {}

    // ─── money rounds ───────────────────────────────────────────────────────

    /// @notice Quorum unlock at the miner's current epoch, so a stale round can't unlock a re-lock.
    function voteUnlock(address miner, uint64 epoch) external nonReentrant onlyValidator {
        Bond storage b = bonds[miner];
        if (!b.locked) revert LockStateUnchanged();
        if (epoch != b.epoch) revert EpochMismatch();

        (uint64 id, uint256 votes, bool quorum) = _castVote(
            keccak256(abi.encode(REQ_UNLOCK, miner)),
            keccak256(abi.encode(REQ_UNLOCK, miner, epoch)),
            _requiredVotes()
        );
        emit VaultVoteCast(msg.sender, REQ_UNLOCK, id, _subject(miner), votes);
        if (!quorum) return;

        uint64 next = epoch + 1;
        b.locked = false;
        b.epoch = next;
        emit BondLockChanged(miner, false, next, solanaPubkeyOf[miner]);
    }

    /// @notice Apply a Solana slash verdict. Rounds are keyed by the full verdict hash, so a junk
    /// verdict opens its own round; the permanent marker allows one application per swapRef.
    function voteSlash(address miner, bytes32 swapRef, uint256 penalty, address user, uint256 reimbursement)
        external
        nonReentrant
        onlyValidator
    {
        if (isSlashed[swapRef]) revert AlreadySlashed();
        if (penalty == 0) revert InvalidAmount();
        if (reimbursement > penalty) revert InvalidReimbursement();
        if (user == address(0)) revert InvalidPayee();

        bytes32 hash = keccak256(abi.encode(REQ_SLASH, miner, swapRef, penalty, user, reimbursement));
        (uint64 id, uint256 votes, bool quorum) = _castVote(hash, hash, _requiredVotes());
        emit VaultVoteCast(msg.sender, REQ_SLASH, id, swapRef, votes);
        if (!quorum) return;

        isSlashed[swapRef] = true;
        Bond storage b = bonds[miner];
        uint256 current = b.collateral;
        uint256 seized = penalty < current ? penalty : current;
        uint256 reimbursed = reimbursement < seized ? reimbursement : seized;
        uint256 surplus = seized - reimbursed;
        b.collateral = uint128(current - seized);
        totalCollateral -= seized;
        accumulatedFees += surplus;

        if (reimbursed > 0) {
            if (user == address(this)) {
                // A ghosted system swap reimburses the vault itself: the USDC is already here, so
                // book it as pot rather than relying on token self-transfer behaviour.
                accumulatedFees += reimbursed;
            } else {
                pendingSlash[swapRef] = PendingSlash(user, uint128(reimbursed));
                pendingSlashTotal += reimbursed;
                if (_push(user, reimbursed)) {
                    delete pendingSlash[swapRef];
                    pendingSlashTotal -= reimbursed;
                } else {
                    emit SlashPending(swapRef, user, reimbursed);
                }
            }
        }
        emit MinerSlashed(miner, swapRef, seized, reimbursed, surplus);
    }

    /// @notice Pay a parked reimbursement to its recorded user. Anyone may trigger it.
    function claimSlash(bytes32 swapRef) external nonReentrant {
        PendingSlash memory rec = pendingSlash[swapRef];
        if (rec.user == address(0)) revert NoPendingSlash();
        delete pendingSlash[swapRef];
        pendingSlashTotal -= rec.amount;
        usdc.safeTransfer(rec.user, rec.amount);
        emit SlashClaimed(swapRef, rec.user, rec.amount);
    }

    /// @notice Settle cumulative protocol fees. Totals are monotonic, so a replayed batch is a no-op;
    /// miners must be strictly ascending, which also rules out duplicates.
    function voteCollectFeesBatch(FeeEntry[] calldata entries) external nonReentrant onlyValidator {
        uint256 len = entries.length;
        if (len == 0 || len > MAX_BATCH) revert InvalidBatch();
        for (uint256 i = 1; i < len; ++i) {
            if (entries[i].miner <= entries[i - 1].miner) revert BatchNotSorted();
        }

        bytes32 hash = keccak256(abi.encode(REQ_COLLECT, entries));
        (uint64 id, uint256 votes, bool quorum) = _castVote(hash, hash, _requiredVotes());
        emit VaultVoteCast(msg.sender, REQ_COLLECT, id, hash, votes);
        if (!quorum) return;

        for (uint256 i; i < len; ++i) {
            address miner = entries[i].miner;
            uint256 cumulative = entries[i].cumulative;
            uint256 prev = settledTotal[miner];
            if (cumulative <= prev) continue;
            uint256 delta = cumulative - prev;
            Bond storage b = bonds[miner];
            uint256 current = b.collateral;
            uint256 collected = delta < current ? delta : current;
            b.collateral = uint128(current - collected);
            totalCollateral -= collected;
            accumulatedFees += collected;
            settledTotal[miner] = cumulative;
            emit FeesSettled(miner, collected, cumulative, delta - collected);
        }
    }

    /// @notice Pay one system-swap lot from the pot to a bonded miner's bond address. Every guard
    /// runs on every vote, so the quorum vote re-checks them all; a failed push leaves the USDC in
    /// the pot and the lot `Failed`.
    function votePayLot(bytes32 lotRef, address miner, uint256 amount, uint64 deadline)
        external
        nonReentrant
        onlyValidator
    {
        _checkLot(lotRef, miner, amount, deadline);
        bytes32 hash = keccak256(abi.encode(REQ_PAY_LOT, lotRef, miner, amount, deadline));

        // One lot round at a time. A dead open lot (round expired or deadline passed) frees the slot.
        bytes32 openRef = openLot.lotRef;
        if (openRef != bytes32(0)) {
            Round storage r = rounds[LOT_ROUND_KEY];
            if (block.timestamp > r.createdAt + config.voteRoundTtl || block.timestamp > openLot.deadline) {
                delete openLot;
                delete rounds[LOT_ROUND_KEY];
                openRef = bytes32(0);
            } else if (r.hash != hash) {
                revert LotAlreadyOpen(openRef);
            }
        }
        if (openRef == bytes32(0)) {
            openLot = OpenLot(lotRef, deadline);
            emit LotOpened(lotRef, miner, amount, deadline, msg.sender);
        }

        (uint64 id, uint256 votes, bool quorum) = _castVote(LOT_ROUND_KEY, hash, _requiredVotes());
        emit VaultVoteCast(msg.sender, REQ_PAY_LOT, id, lotRef, votes);
        if (!quorum) return;

        delete openLot;
        lotStatus[lotRef] = LotStatus.Paid;
        if (_push(miner, amount)) {
            totalLotsPaid += amount;
            uint256 fees = accumulatedFees;
            accumulatedFees = fees > amount ? fees - amount : 0;
            emit LotPaid(lotRef, miner, amount, pot());
        } else {
            lotStatus[lotRef] = LotStatus.Failed;
            emit LotPayFailed(lotRef, miner, amount);
        }
    }

    // ─── governance ─────────────────────────────────────────────────────────

    /// @notice Unanimously approve a candidate, who joins only by calling `acceptValidator` itself.
    function voteAddValidator(address candidate) external nonReentrant onlyValidator {
        _sweepPending();
        if (candidate == address(0)) revert ZeroAddress();
        if (candidate == address(this)) revert InvalidValidatorSet();
        if (isValidator[candidate] || _pendingIndex(candidate) != type(uint256).max) {
            revert AlreadyValidator();
        }
        if (validators.length + pendingValidators.length >= MAX_VALIDATORS) revert InvalidValidatorSet();

        bytes32 set = setHash();
        (uint64 id, uint256 votes, bool quorum) = _castVote(
            keccak256(abi.encode(REQ_ADD_VALIDATOR, candidate)),
            keccak256(abi.encode(REQ_ADD_VALIDATOR, candidate, set)),
            _unanimousVotes()
        );
        emit VaultVoteCast(msg.sender, REQ_ADD_VALIDATOR, id, _subject(candidate), votes);
        if (!quorum) return;

        pendingValidators.push(Pending(candidate, uint64(block.timestamp), set));
        emit ValidatorPending(candidate, uint64(block.timestamp) + config.voteRoundTtl);
    }

    /// @notice The candidate's own call completes its admission, proving it holds the key. Void
    /// after the TTL or after any set change.
    function acceptValidator() external nonReentrant {
        uint256 idx = _pendingIndex(msg.sender);
        if (idx == type(uint256).max) revert NotPendingValidator();
        Pending memory p = pendingValidators[idx];
        if (!_pendingIsLive(p, setHash())) revert AdmissionVoid();

        _removePending(idx);
        validators.push(msg.sender);
        isValidator[msg.sender] = true;
        lastValidatorActivity = uint64(block.timestamp);
        emit ValidatorUpdated(msg.sender, true);
    }

    /// @notice Remove a validator by every other validator. Refused below MIN_VALIDATORS_TO_REMOVE;
    /// the check runs on the completing vote too.
    function voteRemoveValidator(address target) external nonReentrant onlyValidator {
        if (target == msg.sender) revert SelfRemoval();
        if (!isValidator[target]) revert NotValidator();
        uint256 n = validators.length;
        if (n < MIN_VALIDATORS_TO_REMOVE) revert InvalidValidatorSet();

        (uint64 id, uint256 votes, bool quorum) = _castVote(
            keccak256(abi.encode(REQ_REMOVE_VALIDATOR, target)),
            keccak256(abi.encode(REQ_REMOVE_VALIDATOR, target, setHash())),
            n - 1
        );
        emit VaultVoteCast(msg.sender, REQ_REMOVE_VALIDATOR, id, _subject(target), votes);
        if (!quorum) return;

        for (uint256 i; i < n; ++i) {
            if (validators[i] == target) {
                for (uint256 j = i + 1; j < n; ++j) {
                    validators[j - 1] = validators[j];
                }
                validators.pop();
                break;
            }
        }
        isValidator[target] = false;
        emit ValidatorUpdated(target, false);
    }

    /// @notice Replace the whole config. Unanimous, because the config holds the threshold.
    function voteSetConfig(Config calldata cfg) external nonReentrant onlyValidator {
        _checkConfig(cfg);
        bytes32 hash = keccak256(abi.encode(REQ_CONFIG, cfg, setHash()));
        (uint64 id, uint256 votes, bool quorum) = _castVote(hash, hash, _unanimousVotes());
        emit VaultVoteCast(msg.sender, REQ_CONFIG, id, hash, votes);
        if (!quorum) return;

        config = cfg;
        emit ConfigUpdated(cfg);
    }

    // ─── views ──────────────────────────────────────────────────────────────

    /// @notice USDC owed to nobody: balance minus bonds and parked reimbursements, floored at zero.
    function pot() public view returns (uint256) {
        uint256 bal = usdc.balanceOf(address(this));
        uint256 owed = totalCollateral + pendingSlashTotal;
        return bal > owed ? bal - owed : 0;
    }

    function getBond(address miner)
        external
        view
        returns (uint256 collateral, bool locked, uint64 epoch, bytes32 solanaPubkey)
    {
        Bond memory b = bonds[miner];
        return (b.collateral, b.locked, b.epoch, solanaPubkeyOf[miner]);
    }

    function collateralOf(address miner) external view returns (uint256) {
        return bonds[miner].collateral;
    }

    function getConfig() external view returns (Config memory) {
        return config;
    }

    function minCollateral() external view returns (uint256) {
        return config.minCollateral;
    }

    function maxCollateral() external view returns (uint256) {
        return config.maxCollateral;
    }

    function consensusThresholdPct() external view returns (uint8) {
        return config.consensusThresholdPct;
    }

    function voteRoundTtl() external view returns (uint32) {
        return config.voteRoundTtl;
    }

    function maxLotAmount() external view returns (uint256) {
        return config.maxLotAmount;
    }

    function getValidators() external view returns (address[] memory) {
        return validators;
    }

    /// @notice Candidates that can still accept; expired or set-voided admissions are left out.
    function getPendingValidators() external view returns (address[] memory out) {
        uint256 n = pendingValidators.length;
        out = new address[](n);
        uint256 live;
        bytes32 set = setHash();
        for (uint256 i; i < n; ++i) {
            Pending memory p = pendingValidators[i];
            if (_pendingIsLive(p, set)) out[live++] = p.candidate;
        }
        assembly ("memory-safe") {
            mstore(out, live)
        }
    }

    function requiredVotes() external view returns (uint256) {
        return _requiredVotes();
    }

    /// @notice keccak256(abi.encode(validators)); bound into every governance round.
    function setHash() public view returns (bytes32) {
        return keccak256(abi.encode(validators));
    }

    /// @notice True only while the round is live (not expired) and `validator` has voted on it.
    function hasVoted(bytes32 roundKey, address validator) external view returns (bool) {
        Round storage r = rounds[roundKey];
        if (r.id == 0 || block.timestamp > r.createdAt + config.voteRoundTtl) return false;
        uint256 n = r.voters.length;
        for (uint256 i; i < n; ++i) {
            if (r.voters[i] == validator) return true;
        }
        return false;
    }

    function getRound(bytes32 roundKey)
        external
        view
        returns (bytes32 hash, uint64 id, uint64 createdAt, address[] memory voters)
    {
        Round storage r = rounds[roundKey];
        return (r.hash, r.id, r.createdAt, r.voters);
    }

    function roundKeyForUnlock(address miner) external pure returns (bytes32) {
        return keccak256(abi.encode(REQ_UNLOCK, miner));
    }

    function roundKeyForSlash(
        address miner,
        bytes32 swapRef,
        uint256 penalty,
        address user,
        uint256 reimbursement
    ) external pure returns (bytes32) {
        return keccak256(abi.encode(REQ_SLASH, miner, swapRef, penalty, user, reimbursement));
    }

    function roundKeyForCollect(FeeEntry[] calldata entries) external pure returns (bytes32) {
        return keccak256(abi.encode(REQ_COLLECT, entries));
    }

    function roundKeyForPayLot() external pure returns (bytes32) {
        return LOT_ROUND_KEY;
    }

    function roundKeyForAdd(address candidate) external pure returns (bytes32) {
        return keccak256(abi.encode(REQ_ADD_VALIDATOR, candidate));
    }

    function roundKeyForRemove(address target) external pure returns (bytes32) {
        return keccak256(abi.encode(REQ_REMOVE_VALIDATOR, target));
    }

    function roundKeyForConfig(Config calldata cfg, bytes32 validatorSetHash)
        external
        pure
        returns (bytes32)
    {
        return keccak256(abi.encode(REQ_CONFIG, cfg, validatorSetHash));
    }

    // ─── internals ──────────────────────────────────────────────────────────

    function _creditBond(uint256 amount) private returns (uint256 total) {
        if (amount == 0) revert InvalidAmount();
        Bond storage b = bonds[msg.sender];
        total = uint256(b.collateral) + amount;
        uint256 cap = config.maxCollateral;
        if (total > type(uint128).max || (cap != 0 && total > cap)) revert ExceedsMaxCollateral();
        b.collateral = uint128(total);
        totalCollateral += amount;
    }

    /// Credits only what actually arrived; a fee-charging token reverts instead of skewing the books.
    function _pull(uint256 amount) private {
        uint256 before = usdc.balanceOf(address(this));
        usdc.safeTransferFrom(msg.sender, address(this), amount);
        uint256 received = usdc.balanceOf(address(this)) - before;
        if (received != amount) revert DepositMismatch(amount, received);
    }

    /// Fixed-gas transfer that never reverts on token failure and copies at most one return word,
    /// so a gas-burning or return-bombing token can only make the push fail.
    function _push(address to, uint256 amount) private returns (bool success) {
        if (gasleft() < PUSH_GAS_RESERVE) revert InsufficientGas();
        bytes memory data = abi.encodeCall(IERC20.transfer, (to, amount));
        address token = address(usdc);
        uint256 gasLimit = PUSH_GAS;
        assembly ("memory-safe") {
            let ok := call(gasLimit, token, 0, add(data, 0x20), mload(data), 0, 0x20)
            let rds := returndatasize()
            // Success: no return data, or a first word of exactly 1. Anything else is a refusal.
            success := and(ok, or(iszero(rds), and(gt(rds, 31), eq(mload(0), 1))))
        }
    }

    /// Records the caller's vote on the round at `key`, replacing it if expired. Counts only voters
    /// that are still validators, so a removed validator's votes stop counting. Clears on quorum.
    function _castVote(bytes32 key, bytes32 hash, uint256 required)
        private
        returns (uint64 id, uint256 votes, bool quorum)
    {
        Round storage r = rounds[key];
        if (r.id != 0 && block.timestamp > r.createdAt + config.voteRoundTtl) delete rounds[key];
        if (r.id == 0) {
            id = nextRoundId++;
            r.hash = hash;
            r.id = id;
            r.createdAt = uint64(block.timestamp);
        } else {
            if (r.hash != hash) revert PendingConflict();
            id = r.id;
        }

        uint256 n = r.voters.length;
        for (uint256 i; i < n; ++i) {
            address v = r.voters[i];
            if (v == msg.sender) revert AlreadyVoted();
            if (isValidator[v]) ++votes;
        }
        r.voters.push(msg.sender);
        ++votes;

        quorum = votes >= required;
        if (quorum) delete rounds[key];
    }

    function _checkLot(bytes32 lotRef, address miner, uint256 amount, uint64 deadline) private view {
        if (lotRef == bytes32(0)) revert InvalidLotRef();
        if (lotStatus[lotRef] != LotStatus.None) revert LotAlreadySettled();
        if (block.timestamp > deadline) revert LotDeadlinePassed();
        if (deadline > block.timestamp + MAX_LOT_DEADLINE_WINDOW) revert LotDeadlineTooFar();
        if (amount < MIN_LOT_AMOUNT || amount > config.maxLotAmount) revert LotExceedsCap();
        uint256 p = pot();
        if (amount > p) revert LotExceedsPot(amount, p);
        Bond memory b = bonds[miner];
        if (!b.locked || b.collateral < config.minCollateral) revert RecipientNotBonded();
        if (uint256(b.collateral) * 10_000 < amount * LOT_COVERAGE_BPS) revert RecipientUnderCovered();
    }

    function _checkConfig(Config memory c) private pure {
        if (c.minCollateral < MIN_COLLATERAL_FLOOR || c.minCollateral > MIN_COLLATERAL_CEIL) {
            revert ConfigOutOfBounds(0);
        }
        if (
            c.maxCollateral != 0
                && (c.maxCollateral < c.minCollateral || c.maxCollateral > MAX_COLLATERAL_CEIL)
        ) {
            revert ConfigOutOfBounds(1);
        }
        if (c.consensusThresholdPct < MIN_THRESHOLD || c.consensusThresholdPct > MAX_THRESHOLD) {
            revert ConfigOutOfBounds(2);
        }
        if (c.voteRoundTtl < MIN_VOTE_ROUND_TTL || c.voteRoundTtl > MAX_VOTE_ROUND_TTL) {
            revert ConfigOutOfBounds(3);
        }
        if (c.maxLotAmount < MIN_LOT_AMOUNT || c.maxLotAmount > HARD_MAX_LOT_AMOUNT) {
            revert ConfigOutOfBounds(4);
        }
    }

    /// ceil(n * threshold / 100); n >= 1 and threshold >= 51 keep it >= 1.
    function _requiredVotes() private view returns (uint256) {
        return (validators.length * config.consensusThresholdPct + 99) / 100;
    }

    function _unanimousVotes() private view returns (uint256) {
        return validators.length;
    }

    function _pendingIsLive(Pending memory p, bytes32 set) private view returns (bool) {
        return p.setHash == set && block.timestamp <= p.approvedAt + config.voteRoundTtl;
    }

    function _pendingIndex(address candidate) private view returns (uint256) {
        uint256 n = pendingValidators.length;
        for (uint256 i; i < n; ++i) {
            if (pendingValidators[i].candidate == candidate) return i;
        }
        return type(uint256).max;
    }

    function _removePending(uint256 idx) private {
        uint256 n = pendingValidators.length;
        for (uint256 j = idx + 1; j < n; ++j) {
            pendingValidators[j - 1] = pendingValidators[j];
        }
        pendingValidators.pop();
    }

    /// Drops dead admissions so they stop holding a MAX_VALIDATORS slot; order is preserved.
    function _sweepPending() private {
        uint256 n = pendingValidators.length;
        uint256 kept;
        bytes32 set = setHash();
        for (uint256 i; i < n; ++i) {
            Pending memory p = pendingValidators[i];
            if (_pendingIsLive(p, set)) {
                if (kept != i) pendingValidators[kept] = p;
                ++kept;
            }
        }
        while (pendingValidators.length > kept) pendingValidators.pop();
    }

    function _subject(address account) private pure returns (bytes32) {
        return bytes32(uint256(uint160(account)));
    }
}

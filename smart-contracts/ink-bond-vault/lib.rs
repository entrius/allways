#![cfg_attr(not(feature = "std"), no_std, no_main)]

//! allways bond vault — the v2 Bittensor half of the split-collateral design.
//!
//! Solana is the transaction ledger; this contract only custodies TAO bonds,
//! applies validator-quorum slash verdicts relayed from Solana, and enforces
//! the epoch-based active-lock protocol. See
//! `smart-contracts/SOLANA_BITTENSOR_SPLIT_COLLATERAL.md` for the full design.

mod errors;
mod events;

use errors::Error;

// Subtensor chain extension — opentensor/subtensor PR #2560.
// extension=0x1000 and function=18 are upstream-frozen.
#[ink::chain_extension(extension = 0x1000)]
pub trait SubtensorExtension {
    type ErrorCode = SubtensorError;

    #[ink(function = 18)]
    fn add_stake_recycle(
        hotkey: <CustomEnvironment as ink::env::Environment>::AccountId,
        netuid: u16,
        amount: u64,
    ) -> u64;
}

/// Carries subtensor's `Output` status code through instead of flattening —
/// a failed recycle's revert is then diagnosable off-chain.
// allow: SCALE derive's variant-index cast trips clippy on payload variants.
#[allow(clippy::cast_possible_truncation)]
#[ink::scale_derive(Encode, Decode, TypeInfo)]
pub enum SubtensorError {
    Code(u32),
}

impl ink::env::chain_extension::FromStatusCode for SubtensorError {
    fn from_status_code(status_code: u32) -> Result<(), Self> {
        match status_code {
            0 => Ok(()),
            code => Err(Self::Code(code)),
        }
    }
}

// Subtensor's chain Balance is u64 (rao) — verified against the node
// (TotalIssuance storage is 8 bytes); ink's default u128 would trap decoding
// every transferred_value()/transfer(). ink's offchain TEST engine, however,
// is hardcoded to u128 balances, so tests run u128 and the localnet spike
// validates the real u64 config against a real node.
#[cfg(not(test))]
pub(crate) type EnvBalance = u64;
#[cfg(test)]
pub(crate) type EnvBalance = u128;

#[derive(Debug, Clone, PartialEq, Eq)]
#[ink::scale_derive(TypeInfo)]
pub enum CustomEnvironment {}

impl ink::env::Environment for CustomEnvironment {
    const MAX_EVENT_TOPICS: usize =
        <ink::env::DefaultEnvironment as ink::env::Environment>::MAX_EVENT_TOPICS;
    type AccountId = <ink::env::DefaultEnvironment as ink::env::Environment>::AccountId;
    type Balance = EnvBalance;
    type Hash = <ink::env::DefaultEnvironment as ink::env::Environment>::Hash;
    type Timestamp = <ink::env::DefaultEnvironment as ink::env::Environment>::Timestamp;
    type BlockNumber = <ink::env::DefaultEnvironment as ink::env::Environment>::BlockNumber;
    type ChainExtension = SubtensorExtension;
}

#[ink::contract(env = crate::CustomEnvironment)]
mod allways_bond_vault {
    use super::*;
    use events::*;
    use ink::codegen::Env;
    use ink::prelude::vec::Vec;
    use ink::storage::Mapping;

    // Round-type discriminants, bound into every request hash so unlock,
    // slash, fee-settle, and governance rounds can never collide.
    const REQ_UNLOCK: u8 = 0;
    const REQ_SLASH: u8 = 1;
    const REQ_COLLECT: u8 = 2;
    const REQ_ADD_VALIDATOR: u8 = 3;
    const REQ_REMOVE_VALIDATOR: u8 = 4;
    const REQ_CONFIG: u8 = 5;
    const REQ_RECYCLE_TARGET: u8 = 6;

    // Fee-settle batch ceiling: bounds per-entry storage writes so a quorum
    // application always fits a block. One cadence round covers ≤256 miners;
    // larger fleets split into multiple batches.
    const MAX_BATCH: usize = 256;

    // Mirrors the Solana program's MAX_VALIDATORS. Bounds the per-vote set
    // scan and stops unbounded growth on a contract that can never be fixed.
    const MAX_VALIDATORS: usize = 16;

    // Removal is refused below this — at n=2 a removal can strand the set at a
    // single validator, and at n=1 there would be nobody left to vote at all.
    const MIN_VALIDATORS_TO_REMOVE: usize = 3;

    // Floor on the consensus threshold. ceil(n*51/100) is a strict majority at
    // every n, so no quorum can ever be smaller than the honest one — below
    // this a single validator could fabricate a slash and pay itself.
    const MIN_THRESHOLD: u8 = 51;

    // Floor on the configurable round TTL. Quorum-gated config means a
    // *unanimous* mistake could otherwise set a TTL so short that no round can
    // ever gather its votes — unrecoverable, with no owner left to repair it.
    const MIN_VOTE_ROUND_TTL: u32 = 100;

    // Left behind by every recycle so the contract account is never reaped
    // (subtensor ED = 500 rao). Too small only reverts a recycle — retryable,
    // funds never at risk — so an upstream ED raise degrades benignly.
    // Tests carry the offchain engine's own 1_000_000 floor, the same test-env
    // accommodation EnvBalance makes.
    #[cfg(not(test))]
    const ED_RESERVE: Balance = 500;
    #[cfg(test)]
    const ED_RESERVE: Balance = 1_000_000;

    #[ink(storage)]
    pub struct AllwaysBondVault {
        // Configuration. staking_hotkey + netuid are the add_stake_recycle
        // target, movable only by a unanimous round — there is deliberately NO
        // custodial recycle_address fallback here (the pot is ownerless).
        // No chain-ext latch either: #2560 verified live in runtime v443.
        //
        // There is NO owner field and no admin of any kind: every knob below is
        // changed only by a unanimous validator round. See the governance
        // section — that absence is the point, not an omission.
        staking_hotkey: AccountId,
        netuid: u16,
        min_collateral: Balance,
        max_collateral: Balance,
        consensus_threshold_percent: u8,
        // Blocks before an unfinished vote round expires and can be replaced.
        vote_round_ttl: u32,
        // The validator set — Vec for enumeration, capped at MAX_VALIDATORS.
        validators: Vec<AccountId>,
        // Candidates carried by a unanimous add round, awaiting their own
        // accept_validator call. Proving key control before joining keeps a
        // typo'd address from permanently inflating the quorum denominator.
        // (candidate, approval block, approving set hash): the admission
        // expires with the round TTL, and any set change voids it.
        pending_validators: Vec<(AccountId, u32, Hash)>,

        // Miner bonds. `lock_state` is (locked, epoch): epoch increments on
        // every lock/unlock transition and is what the Solana mirror carries —
        // a stale locked-mirror can't authorize activation at a dead epoch.
        collateral: Mapping<AccountId, Balance>,
        lock_state: Mapping<AccountId, (bool, u64)>,

        // Consensus voting, same shape as the v1 contract: voters held as a
        // Vec per request so the round drops in one op on quorum/expiry.
        next_request_id: u64,
        request_voters: Mapping<u64, Vec<AccountId>>,
        request_created: Mapping<u64, u32>,
        request_hash: Mapping<u64, Hash>,
        // One live unlock round per miner. Slash rounds are keyed by the FULL
        // verdict hash: divergent figures open separate rounds rather than
        // conflicting, so no junk vote can park a swap_ref for a whole TTL.
        unlock_request: Mapping<AccountId, u64>,
        slash_request: Mapping<Hash, u64>,
        // Fee-settle rounds keyed by the batch-contents hash, so a cadence
        // batch and an exit's one-entry batch can run concurrently.
        collect_request: Mapping<Hash, u64>,
        // Governance rounds: keyed by the candidate/target account hash for
        // membership, by the config round hash for config.
        governance_request: Mapping<Hash, u64>,

        // Monotonic cumulative protocol fees settled per miner. Monotonicity
        // IS the replay protection: re-applying a batch yields zero deltas.
        settled_total: Mapping<AccountId, Balance>,

        // Permanent replay markers: a swap_ref can be slashed exactly once.
        slashed: Mapping<Hash, bool>,
        // Pull-fallback reimbursements: filled only when the direct push
        // transfer fails, drained by claim_slash.
        pending_slashes: Mapping<Hash, (AccountId, Balance)>,

        // Fee pot (ledger-only accounting; see the fee-recycling design).
        accumulated_fees: Balance,
        total_recycled_fees: Balance,

        // Running sums of everything the vault OWES. Every balance the contract
        // holds above these (plus ED) is recyclable — which is what lets a plain
        // transfer to the address become burn inventory. Keep them exact.
        total_collateral: Balance,
        pending_slash_total: Balance,
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    impl AllwaysBondVault {
        fn ensure_validator(&self) -> Result<(), Error> {
            if !self.validators.contains(&self.env().caller()) {
                return Err(Error::NotValidator);
            }
            Ok(())
        }

        fn get_required_votes(&self) -> u32 {
            let count = u32::try_from(self.validators.len()).unwrap_or(u32::MAX);
            if count == 0 {
                return 1;
            }
            let numerator = count.saturating_mul(self.consensus_threshold_percent as u32);
            let required = numerator.saturating_add(99) / 100;
            core::cmp::max(1, required)
        }

        /// Governance bar: EVERY current validator. Membership and config sit
        /// here rather than at the consensus threshold because the config
        /// *contains* that threshold — a majority able to lower it could
        /// otherwise walk itself down to a quorum of one.
        fn unanimous_votes(&self) -> u32 {
            core::cmp::max(1, u32::try_from(self.validators.len()).unwrap_or(u32::MAX))
        }

        /// Hash of the current set, bound into every governance round so a round
        /// opened under one set can never complete under another. Deliberately
        /// NOT bound into slash/unlock/collect rounds: that would void every
        /// in-flight money round on each membership change.
        fn validator_set_hash(&self) -> Hash {
            Self::hash_request(&self.validators)
        }

        /// A pending admission is live only inside the round TTL AND under the
        /// set that approved it. Either miss and the candidate must be voted
        /// in again — a sleeper must not be able to accept into a later set.
        fn pending_is_live(&self, approved_at: u32, set_hash: Hash) -> bool {
            set_hash == self.validator_set_hash()
                && self.env().block_number() <= approved_at.saturating_add(self.vote_round_ttl)
        }

        /// Drop dead admissions so they stop holding a MAX_VALIDATORS slot.
        /// Called at the head of `vote_add_validator`: the only place that can
        /// run out of slots is also the place that reclaims them.
        fn sweep_pending(&mut self) {
            let now = self.env().block_number();
            let ttl = self.vote_round_ttl;
            let set_hash = self.validator_set_hash();
            self.pending_validators
                .retain(|(_, at, s)| *s == set_hash && now <= at.saturating_add(ttl));
        }

        /// Keccak-hash any SCALE-encodable value. Call sites pass the full
        /// tuple of fields bound into the round — field order must match the
        /// off-chain relayer, so keep these tuples stable when refactoring.
        fn hash_request<T: scale::Encode>(value: &T) -> Hash {
            let mut output =
                <ink::env::hash::Keccak256 as ink::env::hash::HashOutput>::Type::default();
            ink::env::hash_encoded::<ink::env::hash::Keccak256, _>(value, &mut output);
            Hash::from(output)
        }

        fn lock_of(&self, miner: AccountId) -> (bool, u64) {
            self.lock_state.get(miner).unwrap_or((false, 0))
        }

        /// Everything the vault holds that is owed to nobody: settled fees PLUS
        /// any TAO transferred straight to the address. Derived from the real
        /// balance, so it self-heals rather than stranding unattributed funds.
        fn recyclable_pot(&self) -> Balance {
            let owed = self
                .total_collateral
                .saturating_add(self.pending_slash_total)
                .saturating_add(ED_RESERVE);
            self.env().balance().saturating_sub(owed)
        }

        /// AccountId → Hash (both 32 bytes) so `VaultVoteCast.subject` can
        /// carry a miner or a swap_ref through one field.
        fn account_hash(account: &AccountId) -> Hash {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(account.as_ref());
            Hash::from(bytes)
        }

        /// Records the caller's vote and returns the count of votes from
        /// CURRENT validators. Filtering matters: a removed validator's vote
        /// must stop counting, or ejecting a bad actor would leave their
        /// in-flight votes behind against a now-lower bar — making the rounds
        /// they opened cheaper to complete, not harder.
        fn record_vote(&mut self, request_id: u64, caller: AccountId) -> Result<u32, Error> {
            let mut voters = self.request_voters.get(request_id).unwrap_or_default();
            if voters.contains(&caller) {
                return Err(Error::AlreadyVoted);
            }
            voters.push(caller);
            self.request_voters.insert(request_id, &voters);
            let live = voters.iter().filter(|v| self.validators.contains(v)).count();
            Ok(u32::try_from(live).unwrap_or(u32::MAX))
        }

        fn clear_request_data(&mut self, request_id: u64) {
            self.request_voters.remove(request_id);
            self.request_created.remove(request_id);
            self.request_hash.remove(request_id);
        }

        /// Resolve the live round id for a key's mapping entry, expiring stale
        /// rounds. Returns None when a fresh round must be allocated.
        fn live_round(&mut self, id: Option<u64>) -> Option<u64> {
            let id = id?;
            let created = self.request_created.get(id).unwrap_or(0);
            if self.env().block_number() > created.saturating_add(self.vote_round_ttl) {
                self.clear_request_data(id);
                return None;
            }
            Some(id)
        }

        fn new_round(&mut self, hash: Hash) -> u64 {
            let id = self.next_request_id;
            self.next_request_id = id.saturating_add(1);
            self.request_hash.insert(id, &hash);
            self.request_created.insert(id, &self.env().block_number());
            id
        }

        /// Vote on the round identified by `existing` (allocating a new one if
        /// absent/expired), enforcing hash equality across the round. Returns
        /// (round_id, vote_count, quorum_reached).
        fn cast_vote(
            &mut self,
            existing: Option<u64>,
            round_hash: Hash,
        ) -> Result<(u64, u32, bool), Error> {
            self.cast_vote_with(existing, round_hash, self.get_required_votes())
        }

        /// As `cast_vote`, with an explicit bar — governance rounds pass the
        /// unanimity count instead of the consensus threshold.
        fn cast_vote_with(
            &mut self,
            existing: Option<u64>,
            round_hash: Hash,
            required: u32,
        ) -> Result<(u64, u32, bool), Error> {
            let caller = self.env().caller();
            let id = match self.live_round(existing) {
                Some(id) => {
                    if self.request_hash.get(id).unwrap_or_default() != round_hash {
                        return Err(Error::PendingConflict);
                    }
                    id
                }
                None => self.new_round(round_hash),
            };
            let votes = self.record_vote(id, caller)?;
            Ok((id, votes, votes >= required))
        }
    }

    impl AllwaysBondVault {
        /// Fallible by design: the seed set is the ONLY way validators ever come
        /// to exist, so a bad one is unrecoverable on an immutable contract.
        /// An empty set would let miners bond and lock with nobody able to ever
        /// unlock them; a duplicate would inflate the quorum denominator past
        /// what the real signers can reach. Both brick the vault, so both fail
        /// the deployment instead.
        #[ink(constructor)]
        pub fn new(
            staking_hotkey: AccountId,
            netuid: u16,
            min_collateral: Balance,
            max_collateral: Balance,
            consensus_threshold_percent: u8,
            vote_round_ttl: u32,
            validators: Vec<AccountId>,
        ) -> Result<Self, Error> {
            if validators.is_empty() || validators.len() > MAX_VALIDATORS {
                return Err(Error::InvalidValidatorSet);
            }
            for (i, v) in validators.iter().enumerate() {
                if validators.iter().skip(i.saturating_add(1)).any(|o| o == v) {
                    return Err(Error::InvalidValidatorSet);
                }
            }
            if consensus_threshold_percent > 100 {
                return Err(Error::InvalidAmount);
            }
            if consensus_threshold_percent < MIN_THRESHOLD {
                return Err(Error::ThresholdTooLow);
            }
            if vote_round_ttl < MIN_VOTE_ROUND_TTL {
                return Err(Error::InvalidAmount);
            }
            Ok(Self {
                staking_hotkey,
                netuid,
                min_collateral,
                max_collateral,
                consensus_threshold_percent,
                vote_round_ttl,
                validators,
                pending_validators: Vec::new(),

                collateral: Mapping::default(),
                lock_state: Mapping::default(),

                next_request_id: 1,
                request_voters: Mapping::default(),
                request_created: Mapping::default(),
                request_hash: Mapping::default(),
                unlock_request: Mapping::default(),
                slash_request: Mapping::default(),
                collect_request: Mapping::default(),
                governance_request: Mapping::default(),
                settled_total: Mapping::default(),

                slashed: Mapping::default(),
                pending_slashes: Mapping::default(),

                accumulated_fees: 0,
                total_recycled_fees: 0,
                total_collateral: 0,
                pending_slash_total: 0,
            })
        }

        // =====================================================================
        // Bond management (miner direct — caller-based auth)
        // =====================================================================

        #[ink(message, payable)]
        pub fn post_collateral(&mut self) -> Result<(), Error> {
            let caller = self.env().caller();
            let amount = self.env().transferred_value();
            if amount == 0 {
                return Err(Error::InvalidAmount);
            }

            let current = self.collateral.get(caller).unwrap_or(0);
            let new_total = current.saturating_add(amount);
            if self.max_collateral > 0 && new_total > self.max_collateral {
                return Err(Error::ExceedsMaxCollateral);
            }
            self.collateral.insert(caller, &new_total);
            self.total_collateral = self.total_collateral.saturating_add(amount);

            self.env().emit_event(CollateralPosted {
                miner: caller,
                amount: amount.into(),
                total: new_total.into(),
            });
            Ok(())
        }

        /// Withdraw is refused while locked — the only path back to unlocked
        /// is deactivating on Solana and a validator `vote_unlock` quorum, so
        /// this single check carries the whole cross-chain invariant.
        #[ink(message)]
        pub fn withdraw_collateral(&mut self, amount: Balance) -> Result<(), Error> {
            let caller = self.env().caller();
            if amount == 0 {
                return Err(Error::InvalidAmount);
            }
            let (locked, _) = self.lock_of(caller);
            if locked {
                return Err(Error::BondLocked);
            }

            let current = self.collateral.get(caller).unwrap_or(0);
            if amount > current {
                return Err(Error::InsufficientCollateral);
            }

            let remaining = current.saturating_sub(amount);
            self.collateral.insert(caller, &remaining);
            self.total_collateral = self.total_collateral.saturating_sub(amount);
            self.env()
                .transfer(caller, amount)
                .map_err(|_| Error::TransferFailed)?;

            self.env().emit_event(CollateralWithdrawn {
                miner: caller,
                amount: amount.into(),
                remaining: remaining.into(),
            });
            Ok(())
        }

        // =====================================================================
        // Active-lock protocol
        // =====================================================================

        /// Miner-initiated lock — locking yourself is always safe, so no
        /// quorum. A locked bond ≥ min_collateral is what the mirror relays to
        /// Solana as "eligible for vote_activate". Deliberately un-gated on set
        /// size: unlock at n=1 is 1-of-1 and always live, so a small set is a
        /// trust choice, not a stranding risk.
        #[ink(message)]
        pub fn lock_bond(&mut self) -> Result<(), Error> {
            let caller = self.env().caller();
            let (locked, epoch) = self.lock_of(caller);
            if locked {
                return Err(Error::LockStateUnchanged);
            }
            if self.collateral.get(caller).unwrap_or(0) < self.min_collateral {
                return Err(Error::InsufficientCollateral);
            }
            let next_epoch = epoch.saturating_add(1);
            self.lock_state.insert(caller, &(true, next_epoch));
            self.env().emit_event(BondLockChanged {
                miner: caller,
                locked: true,
                epoch: next_epoch,
            });
            Ok(())
        }

        /// Validator-quorum unlock. Preconditions live off-chain: validators
        /// only vote once the miner is deactivated on Solana AND fully
        /// quiescent (no in-flight swaps, timeout windows past, every slash
        /// verdict applied here). The epoch is hash-bound into the round so a
        /// stale round can never unlock a re-locked bond.
        #[ink(message)]
        pub fn vote_unlock(&mut self, miner: AccountId, epoch: u64) -> Result<(), Error> {
            self.ensure_validator()?;

            let (locked, current_epoch) = self.lock_of(miner);
            if !locked {
                return Err(Error::LockStateUnchanged);
            }
            if epoch != current_epoch {
                return Err(Error::EpochMismatch);
            }

            let round_hash = Self::hash_request(&(REQ_UNLOCK, miner, epoch));
            let existing = self.unlock_request.get(miner);
            let (id, votes, quorum) = self.cast_vote(existing, round_hash)?;
            self.unlock_request.insert(miner, &id);

            self.env().emit_event(VaultVoteCast {
                validator: self.env().caller(),
                req_type: REQ_UNLOCK,
                request_id: id,
                subject: Self::account_hash(&miner),
                vote_count: votes,
            });

            if quorum {
                let next_epoch = current_epoch.saturating_add(1);
                self.lock_state.insert(miner, &(false, next_epoch));
                self.clear_request_data(id);
                self.unlock_request.remove(miner);
                self.env().emit_event(BondLockChanged {
                    miner,
                    locked: false,
                    epoch: next_epoch,
                });
            }
            Ok(())
        }

        // =====================================================================
        // Slashing (relayed Solana timeout verdicts)
        // =====================================================================

        /// Apply a slash verdict relayed from Solana. `swap_ref` is the Solana
        /// swap key (32 bytes); the round hash binds every argument so all
        /// validators must relay the identical verdict, and it KEYS the round
        /// too — a junk verdict opens its own round instead of parking the
        /// swap_ref's for a TTL, and the `slashed` marker still allows exactly
        /// one application ever. On quorum: seize
        /// min(penalty, collateral), reimburse the wronged user (push, with a
        /// pull fallback via claim_slash so a failed transfer can't revert the
        /// quorum application), credit any surplus to the fee pot.
        #[ink(message)]
        pub fn vote_slash(
            &mut self,
            miner: AccountId,
            swap_ref: Hash,
            penalty: Balance,
            user: AccountId,
            reimbursement: Balance,
        ) -> Result<(), Error> {
            self.ensure_validator()?;
            if self.slashed.get(swap_ref).unwrap_or(false) {
                return Err(Error::AlreadySlashed);
            }
            if penalty == 0 {
                return Err(Error::InvalidAmount);
            }
            if reimbursement > penalty {
                return Err(Error::InvalidReimbursement);
            }

            let round_hash =
                Self::hash_request(&(REQ_SLASH, miner, swap_ref, penalty, user, reimbursement));
            let existing = self.slash_request.get(round_hash);
            let (id, votes, quorum) = self.cast_vote(existing, round_hash)?;
            self.slash_request.insert(round_hash, &id);

            self.env().emit_event(VaultVoteCast {
                validator: self.env().caller(),
                req_type: REQ_SLASH,
                request_id: id,
                subject: swap_ref,
                vote_count: votes,
            });

            if quorum {
                // Permanent marker first: this swap_ref can never slash again,
                // whatever other verdict rounds are open against it.
                self.slashed.insert(swap_ref, &true);
                self.clear_request_data(id);
                self.slash_request.remove(round_hash);

                let current = self.collateral.get(miner).unwrap_or(0);
                let seized = core::cmp::min(penalty, current);
                let reimbursed = core::cmp::min(reimbursement, seized);
                let surplus = seized.saturating_sub(reimbursed);

                self.collateral
                    .insert(miner, &current.saturating_sub(seized));
                self.total_collateral = self.total_collateral.saturating_sub(seized);
                self.accumulated_fees = self.accumulated_fees.saturating_add(surplus);

                if reimbursed > 0 && self.env().transfer(user, reimbursed).is_err() {
                    self.pending_slashes
                        .insert(swap_ref, &(user, reimbursed));
                    self.pending_slash_total =
                        self.pending_slash_total.saturating_add(reimbursed);
                    self.env().emit_event(SlashPending {
                        swap_ref,
                        user,
                        amount: reimbursed.into(),
                    });
                }

                self.env().emit_event(MinerSlashed {
                    miner,
                    swap_ref,
                    seized: seized.into(),
                    reimbursed: reimbursed.into(),
                    surplus: surplus.into(),
                });
            }
            Ok(())
        }

        /// Settle accrued protocol fees onto the vault's books — the relayed
        /// half of the fee pipeline (validators compute totals from Solana
        /// swap history). `entries` = (miner, cumulative_total): monotonic
        /// per-miner totals, so stale entries no-op and replay is harmless.
        /// Rounds are keyed by the batch-contents hash: a cadence batch and an
        /// exit's one-entry batch can run concurrently. Collected amounts are
        /// clamped to remaining collateral; any shortfall (bond eaten by a
        /// slash below owed fees) is written off and reported in the event so
        /// settlement can never wedge an exit. Deliberately NOT halt-gated:
        /// settlement is exit-path (halt gates entry, never exit).
        #[ink(message)]
        pub fn vote_collect_fees_batch(
            &mut self,
            entries: Vec<(AccountId, Balance)>,
        ) -> Result<(), Error> {
            self.ensure_validator()?;
            if entries.is_empty() || entries.len() > MAX_BATCH {
                return Err(Error::InvalidBatch);
            }

            let round_hash = Self::hash_request(&(REQ_COLLECT, &entries));
            let existing = self.collect_request.get(round_hash);
            let (id, votes, quorum) = self.cast_vote(existing, round_hash)?;
            self.collect_request.insert(round_hash, &id);

            self.env().emit_event(VaultVoteCast {
                validator: self.env().caller(),
                req_type: REQ_COLLECT,
                request_id: id,
                subject: round_hash,
                vote_count: votes,
            });

            if quorum {
                self.clear_request_data(id);
                self.collect_request.remove(round_hash);

                for (miner, new_total) in entries {
                    let prev = self.settled_total.get(miner).unwrap_or(0);
                    if new_total <= prev {
                        continue;
                    }
                    let delta = new_total.saturating_sub(prev);
                    let current = self.collateral.get(miner).unwrap_or(0);
                    let collected = core::cmp::min(delta, current);
                    let shortfall = delta.saturating_sub(collected);

                    self.collateral
                        .insert(miner, &current.saturating_sub(collected));
                    self.total_collateral = self.total_collateral.saturating_sub(collected);
                    self.accumulated_fees = self.accumulated_fees.saturating_add(collected);
                    self.settled_total.insert(miner, &new_total);

                    self.env().emit_event(FeesSettled {
                        miner,
                        collected: collected.into(),
                        new_cumulative: new_total.into(),
                        shortfall: shortfall.into(),
                    });
                }
            }
            Ok(())
        }

        /// Claim a reimbursement whose direct push transfer failed.
        #[ink(message)]
        pub fn claim_slash(&mut self, swap_ref: Hash) -> Result<(), Error> {
            let caller = self.env().caller();
            let (user, amount) = self
                .pending_slashes
                .get(swap_ref)
                .ok_or(Error::NoPendingSlash)?;
            if user != caller {
                return Err(Error::InvalidStatus);
            }

            self.pending_slashes.remove(swap_ref);
            self.pending_slash_total = self.pending_slash_total.saturating_sub(amount);
            self.env().transfer(caller, amount).map_err(|_| {
                self.pending_slashes.insert(swap_ref, &(user, amount));
                self.pending_slash_total = self.pending_slash_total.saturating_add(amount);
                Error::TransferFailed
            })?;

            self.env().emit_event(SlashClaimed {
                swap_ref,
                user: caller,
                amount: amount.into(),
            });
            Ok(())
        }

        // =====================================================================
        // Fee recycling
        // =====================================================================

        /// Permissionless, caller-pays. Drains everything the vault holds that
        /// is owed to nobody — settled fees AND any TAO sent straight to the
        /// address — into add_stake_recycle(staking_hotkey, netuid). There is
        /// no other destination and no owner path to the funds.
        #[ink(message)]
        pub fn recycle_fees(&mut self) -> Result<(), Error> {
            let pot = self.recyclable_pot();
            if pot == 0 {
                return Err(Error::InvalidAmount);
            }
            let donated = pot.saturating_sub(self.accumulated_fees);

            // try_into is a no-op on-chain (Balance = u64) but real under the
            // test env's u128 Balance — keep it despite clippy.
            #[allow(clippy::useless_conversion)]
            let amount: u64 = pot.try_into().map_err(|_| Error::TransferFailed)?;
            self.env()
                .extension()
                .add_stake_recycle(self.staking_hotkey, self.netuid, amount)
                .map_err(|SubtensorError::Code(c)| Error::ChainExtension(c))?;

            // Saturating, not zeroing: an ED-clamped pot leaves the unrecycled
            // remainder on the books instead of erasing the fee record.
            self.accumulated_fees = self.accumulated_fees.saturating_sub(pot);
            self.total_recycled_fees = self.total_recycled_fees.saturating_add(pot);
            self.env().emit_event(FeesRecycled {
                tao_amount: pot.into(),
                donated: donated.into(),
            });
            Ok(())
        }

        // =====================================================================
        // Governance — validator set + config, by validator quorum only.
        // There is no owner and no admin key: nothing below can be reached by
        // any single actor, and nothing anywhere in this contract can move a
        // miner's funds except a slash/unlock quorum or the miner themselves.
        // =====================================================================

        /// Carry a candidate to the pending list. UNANIMOUS: a bare majority
        /// must never be able to pack the set with its own sybils and then vote
        /// itself whatever it likes.
        #[ink(message)]
        pub fn vote_add_validator(&mut self, candidate: AccountId) -> Result<(), Error> {
            self.ensure_validator()?;
            self.sweep_pending();
            if self.validators.contains(&candidate)
                || self.pending_validators.iter().any(|(c, _, _)| c == &candidate)
            {
                return Err(Error::AlreadyValidator);
            }
            if self.validators.len().saturating_add(self.pending_validators.len()) >= MAX_VALIDATORS
            {
                return Err(Error::InvalidValidatorSet);
            }

            let round_hash =
                Self::hash_request(&(REQ_ADD_VALIDATOR, candidate, self.validator_set_hash()));
            let existing = self.governance_request.get(Self::account_hash(&candidate));
            let (id, votes, quorum) =
                self.cast_vote_with(existing, round_hash, self.unanimous_votes())?;
            self.governance_request
                .insert(Self::account_hash(&candidate), &id);

            self.env().emit_event(VaultVoteCast {
                validator: self.env().caller(),
                req_type: REQ_ADD_VALIDATOR,
                request_id: id,
                subject: Self::account_hash(&candidate),
                vote_count: votes,
            });

            if quorum {
                self.clear_request_data(id);
                self.governance_request.remove(Self::account_hash(&candidate));
                let approved_at = self.env().block_number();
                let set_hash = self.validator_set_hash();
                self.pending_validators
                    .push((candidate, approved_at, set_hash));
                self.env().emit_event(ValidatorPending {
                    candidate,
                    expires_at: approved_at.saturating_add(self.vote_round_ttl),
                });
            }
            Ok(())
        }

        /// The candidate's own signature completes their admission — proof they
        /// hold the key. Without it a typo'd address would join the set, count
        /// toward every quorum, and never be able to vote.
        ///
        /// Bounded by the round TTL and the approving set: a candidate who sits
        /// on an approval cannot surface later and land in a set that never
        /// agreed to them, holding every money quorum hostage.
        #[ink(message)]
        pub fn accept_validator(&mut self) -> Result<(), Error> {
            let caller = self.env().caller();
            let (_, approved_at, set_hash) = *self
                .pending_validators
                .iter()
                .find(|(c, _, _)| c == &caller)
                .ok_or(Error::NotPendingValidator)?;
            if !self.pending_is_live(approved_at, set_hash) {
                return Err(Error::AdmissionVoid);
            }
            self.pending_validators.retain(|(c, _, _)| c != &caller);
            self.validators.push(caller);
            self.env().emit_event(ValidatorUpdated {
                validator: caller,
                registered: true,
            });
            Ok(())
        }

        /// Eject a validator. Requires every OTHER validator — the target is
        /// barred from voting, so a dark or compromised key can still be
        /// removed by the rest. Refused below MIN_VALIDATORS_TO_REMOVE, since
        /// concurrent removals could otherwise walk the set down to one.
        #[ink(message)]
        pub fn vote_remove_validator(&mut self, validator: AccountId) -> Result<(), Error> {
            self.ensure_validator()?;
            if self.env().caller() == validator {
                return Err(Error::SelfRemoval);
            }
            if !self.validators.contains(&validator) {
                return Err(Error::NotValidator);
            }
            if self.validators.len() < MIN_VALIDATORS_TO_REMOVE {
                return Err(Error::InvalidValidatorSet);
            }

            let round_hash =
                Self::hash_request(&(REQ_REMOVE_VALIDATOR, validator, self.validator_set_hash()));
            let existing = self.governance_request.get(Self::account_hash(&validator));
            // Everyone except the target.
            let required = self.unanimous_votes().saturating_sub(1).max(1);
            let (id, votes, quorum) = self.cast_vote_with(existing, round_hash, required)?;
            self.governance_request
                .insert(Self::account_hash(&validator), &id);

            self.env().emit_event(VaultVoteCast {
                validator: self.env().caller(),
                req_type: REQ_REMOVE_VALIDATOR,
                request_id: id,
                subject: Self::account_hash(&validator),
                vote_count: votes,
            });

            if quorum {
                // Re-check at APPLICATION: two rounds opened at the floor could
                // otherwise complete in sequence and strand the set below it.
                if self.validators.len() < MIN_VALIDATORS_TO_REMOVE {
                    return Err(Error::InvalidValidatorSet);
                }
                self.clear_request_data(id);
                self.governance_request.remove(Self::account_hash(&validator));
                self.validators.retain(|v| v != &validator);
                self.env().emit_event(ValidatorUpdated {
                    validator,
                    registered: false,
                });
            }
            Ok(())
        }

        /// Set the whole config in one unanimous round. Whole, not delta: the
        /// round hash binds every field, so validators agree on the RESULTING
        /// config rather than on an edit applied to whatever they each last
        /// read. Unanimous because this tuple contains the consensus threshold.
        #[ink(message)]
        pub fn vote_set_config(
            &mut self,
            min_collateral: Balance,
            max_collateral: Balance,
            consensus_threshold_percent: u8,
            vote_round_ttl: u32,
        ) -> Result<(), Error> {
            self.ensure_validator()?;
            if consensus_threshold_percent > 100 {
                return Err(Error::InvalidAmount);
            }
            if consensus_threshold_percent < MIN_THRESHOLD {
                return Err(Error::ThresholdTooLow);
            }
            if vote_round_ttl < MIN_VOTE_ROUND_TTL {
                return Err(Error::InvalidAmount);
            }

            let payload = (
                REQ_CONFIG,
                min_collateral,
                max_collateral,
                consensus_threshold_percent,
                vote_round_ttl,
                self.validator_set_hash(),
            );
            let round_hash = Self::hash_request(&payload);
            let existing = self.governance_request.get(round_hash);
            let (id, votes, quorum) =
                self.cast_vote_with(existing, round_hash, self.unanimous_votes())?;
            self.governance_request.insert(round_hash, &id);

            self.env().emit_event(VaultVoteCast {
                validator: self.env().caller(),
                req_type: REQ_CONFIG,
                request_id: id,
                subject: round_hash,
                vote_count: votes,
            });

            if quorum {
                self.clear_request_data(id);
                self.governance_request.remove(round_hash);
                self.min_collateral = min_collateral;
                self.max_collateral = max_collateral;
                self.consensus_threshold_percent = consensus_threshold_percent;
                self.vote_round_ttl = vote_round_ttl;
                self.env().emit_event(ConfigUpdated {
                    min_collateral: min_collateral.into(),
                    max_collateral: max_collateral.into(),
                    consensus_threshold_percent,
                    vote_round_ttl,
                });
            }
            Ok(())
        }

        /// Move the `add_stake_recycle` destination in one unanimous round.
        /// A hotkey that stops being stakeable (swap_hotkey, deregistration, an
        /// upstream rule change) would otherwise revert every recycle forever,
        /// with no owner and no upgrade path to repair it. Hotkey and netuid
        /// move together: a hotkey is only registered on some subnets.
        #[ink(message)]
        pub fn vote_set_recycle_target(
            &mut self,
            staking_hotkey: AccountId,
            netuid: u16,
        ) -> Result<(), Error> {
            self.ensure_validator()?;

            let payload = (
                REQ_RECYCLE_TARGET,
                staking_hotkey,
                netuid,
                self.validator_set_hash(),
            );
            let round_hash = Self::hash_request(&payload);
            let existing = self.governance_request.get(round_hash);
            let (id, votes, quorum) =
                self.cast_vote_with(existing, round_hash, self.unanimous_votes())?;
            self.governance_request.insert(round_hash, &id);

            self.env().emit_event(VaultVoteCast {
                validator: self.env().caller(),
                req_type: REQ_RECYCLE_TARGET,
                request_id: id,
                subject: round_hash,
                vote_count: votes,
            });

            if quorum {
                self.clear_request_data(id);
                self.governance_request.remove(round_hash);
                self.staking_hotkey = staking_hotkey;
                self.netuid = netuid;
                self.env().emit_event(RecycleTargetUpdated {
                    staking_hotkey,
                    netuid,
                });
            }
            Ok(())
        }

        // =====================================================================
        // Queries
        // =====================================================================

        #[ink(message)]
        pub fn get_collateral(&self, miner: AccountId) -> Balance {
            self.collateral.get(miner).unwrap_or(0)
        }

        /// (locked, epoch) — exactly the tuple the mirror relays to Solana
        /// alongside the balance.
        #[ink(message)]
        pub fn get_lock_state(&self, miner: AccountId) -> (bool, u64) {
            self.lock_of(miner)
        }

        #[ink(message)]
        pub fn is_slashed(&self, swap_ref: Hash) -> bool {
            self.slashed.get(swap_ref).unwrap_or(false)
        }

        #[ink(message)]
        pub fn get_pending_slash(&self, swap_ref: Hash) -> Option<(AccountId, Balance)> {
            self.pending_slashes.get(swap_ref)
        }

        /// Cumulative protocol fees settled for this miner (the vault-side
        /// half of the monotonic total the relayer tracks).
        #[ink(message)]
        pub fn get_settled_total(&self, miner: AccountId) -> Balance {
            self.settled_total.get(miner).unwrap_or(0)
        }

        #[ink(message)]
        pub fn get_accumulated_fees(&self) -> Balance {
            self.accumulated_fees
        }

        #[ink(message)]
        pub fn get_total_recycled_fees(&self) -> Balance {
            self.total_recycled_fees
        }

        /// Exactly what the next `recycle_fees` would drain: settled fees plus
        /// unattributed TAO. Reads ≥ `get_accumulated_fees` whenever anyone has
        /// donated; the difference is the donated share.
        #[ink(message)]
        pub fn get_recyclable_pot(&self) -> Balance {
            self.recyclable_pot()
        }

        /// Total bonds owed to miners — the commingling invariant's first term.
        #[ink(message)]
        pub fn get_total_collateral(&self) -> Balance {
            self.total_collateral
        }

        /// Unclaimed slash reimbursements owed to users.
        #[ink(message)]
        pub fn get_pending_slash_total(&self) -> Balance {
            self.pending_slash_total
        }

        #[ink(message)]
        pub fn get_validators(&self) -> Vec<AccountId> {
            self.validators.clone()
        }

        /// Candidates a unanimous round approved and who can still accept —
        /// expired or set-voided admissions are omitted, since neither can ever
        /// join. They count toward no quorum until they accept.
        #[ink(message)]
        pub fn get_pending_validators(&self) -> Vec<AccountId> {
            self.pending_validators
                .iter()
                .filter(|(_, at, s)| self.pending_is_live(*at, *s))
                .map(|(c, _, _)| *c)
                .collect()
        }

        #[ink(message)]
        pub fn get_vote_round_ttl(&self) -> u32 {
            self.vote_round_ttl
        }

        #[ink(message)]
        pub fn get_min_collateral(&self) -> Balance {
            self.min_collateral
        }

        #[ink(message)]
        pub fn get_max_collateral(&self) -> Balance {
            self.max_collateral
        }

        #[ink(message)]
        pub fn get_consensus_threshold(&self) -> u8 {
            self.consensus_threshold_percent
        }

        #[ink(message)]
        pub fn get_staking_hotkey(&self) -> AccountId {
            self.staking_hotkey
        }

        #[ink(message)]
        pub fn get_netuid(&self) -> u16 {
            self.netuid
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        const MIN_COLLATERAL: Balance = 1_000;

        fn accounts() -> ink::env::test::DefaultAccounts<ink::env::DefaultEnvironment> {
            ink::env::test::default_accounts::<ink::env::DefaultEnvironment>()
        }

        fn set_caller(caller: AccountId) {
            ink::env::test::set_caller::<ink::env::DefaultEnvironment>(caller);
        }

        fn contract_id() -> AccountId {
            ink::env::test::callee::<ink::env::DefaultEnvironment>()
        }

        fn contract_balance() -> Balance {
            ink::env::test::get_account_balance::<crate::CustomEnvironment>(contract_id())
                .unwrap_or(0)
        }

        /// The offchain engine does not credit the callee for a payable call,
        /// so every test that pays the vault must move the balance itself —
        /// which the recyclable pot now reads.
        fn credit_contract(amount: Balance) {
            ink::env::test::set_account_balance::<crate::CustomEnvironment>(
                contract_id(),
                contract_balance().saturating_add(amount),
            );
        }

        /// Vault seeded with (django, eve) as validators, threshold 100% ⇒
        /// quorum = 2 and unanimity = 2. Endowed with exactly ED, as a real
        /// instantiation is — so the pot starts at zero.
        fn new_vault() -> AllwaysBondVault {
            seeded_vault(ink::prelude::vec![accounts().django, accounts().eve])
        }

        fn seeded_vault(validators: Vec<AccountId>) -> AllwaysBondVault {
            seeded_vault_at(validators, 100)
        }

        /// At a majority threshold an honest quorum carries a verdict without
        /// the dissenter — which is what the anti-grief rounds are about.
        fn seeded_vault_at(validators: Vec<AccountId>, threshold: u8) -> AllwaysBondVault {
            let acc = accounts();
            set_caller(acc.alice);
            ink::env::test::set_account_balance::<crate::CustomEnvironment>(
                contract_id(),
                ED_RESERVE,
            );
            AllwaysBondVault::new(acc.frank, 7, MIN_COLLATERAL, 0, threshold, 100, validators)
                .unwrap()
        }

        fn post(vault: &mut AllwaysBondVault, miner: AccountId, amount: Balance) {
            set_caller(miner);
            ink::env::test::set_value_transferred::<crate::CustomEnvironment>(amount);
            credit_contract(amount);
            vault.post_collateral().unwrap();
            ink::env::test::set_value_transferred::<crate::CustomEnvironment>(0);
        }

        fn advance_blocks(n: u32) {
            for _ in 0..n {
                ink::env::test::advance_block::<crate::CustomEnvironment>();
            }
        }

        /// TAO arriving with no claim against it — a plain transfer to the
        /// address, which calls no code.
        fn donate_raw(amount: Balance) {
            credit_contract(amount);
        }

        /// Stands in for the balance the real chain extension moves out; the
        /// mock cannot do it itself (see MockRecycleExt).
        fn debit_contract(amount: Balance) {
            ink::env::test::set_account_balance::<crate::CustomEnvironment>(
                contract_id(),
                contract_balance().saturating_sub(amount),
            );
        }

        #[ink::test]
        fn lock_blocks_withdraw_until_quorum_unlock() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django, acc.eve, acc.charlie]);
            post(&mut vault, acc.bob, 5_000);

            set_caller(acc.bob);
            vault.lock_bond().unwrap();
            assert_eq!(vault.get_lock_state(acc.bob), (true, 1));
            assert_eq!(vault.withdraw_collateral(1_000), Err(Error::BondLocked));

            // Below quorum the bond stays locked.
            set_caller(acc.django);
            vault.vote_unlock(acc.bob, 1).unwrap();
            set_caller(acc.eve);
            vault.vote_unlock(acc.bob, 1).unwrap();
            assert_eq!(vault.get_lock_state(acc.bob), (true, 1));

            // Final vote reaches quorum: unlocked, epoch bumped.
            set_caller(acc.charlie);
            vault.vote_unlock(acc.bob, 1).unwrap();
            assert_eq!(vault.get_lock_state(acc.bob), (false, 2));

            set_caller(acc.bob);
            vault.withdraw_collateral(1_000).unwrap();
            assert_eq!(vault.get_collateral(acc.bob), 4_000);
        }

        #[ink::test]
        fn unlock_vote_requires_current_epoch() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django, acc.eve, acc.charlie]);
            post(&mut vault, acc.bob, 5_000);
            set_caller(acc.bob);
            vault.lock_bond().unwrap();

            set_caller(acc.django);
            assert_eq!(vault.vote_unlock(acc.bob, 0), Err(Error::EpochMismatch));
            assert_eq!(vault.vote_unlock(acc.bob, 2), Err(Error::EpochMismatch));
        }

        #[ink::test]
        fn lock_requires_min_collateral() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django, acc.eve, acc.charlie]);
            post(&mut vault, acc.bob, MIN_COLLATERAL - 1);
            set_caller(acc.bob);
            assert_eq!(vault.lock_bond(), Err(Error::InsufficientCollateral));
        }

        /// Launch runs a single validator for months, so locking must work at
        /// n=1 — where unlock is 1-of-1 and the bond can never be stranded.
        #[ink::test]
        fn lone_validator_can_lock_and_unlock_a_bond() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django]);
            post(&mut vault, acc.bob, 5_000);
            set_caller(acc.bob);
            vault.lock_bond().unwrap();
            assert_eq!(vault.get_lock_state(acc.bob), (true, 1));

            set_caller(acc.django);
            vault.vote_unlock(acc.bob, 1).unwrap();
            assert_eq!(vault.get_lock_state(acc.bob), (false, 2));
        }

        /// The set size a lock lands in is a trust choice, not a precondition:
        /// n=2 is the ramp between launch and the removal floor.
        #[ink::test]
        fn lock_succeeds_below_the_removal_floor() {
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);
            set_caller(acc.bob);
            vault.lock_bond().unwrap();
            assert_eq!(vault.get_lock_state(acc.bob), (true, 1));
        }

        #[ink::test]
        fn slash_reimburses_user_and_credits_surplus() {
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);

            let swap_ref = Hash::from([7u8; 32]);
            set_caller(acc.django);
            vault
                .vote_slash(acc.bob, swap_ref, 3_000, acc.charlie, 2_500)
                .unwrap();
            // Not yet quorum — nothing applied.
            assert_eq!(vault.get_collateral(acc.bob), 5_000);
            assert!(!vault.is_slashed(swap_ref));

            set_caller(acc.eve);
            vault
                .vote_slash(acc.bob, swap_ref, 3_000, acc.charlie, 2_500)
                .unwrap();
            assert!(vault.is_slashed(swap_ref));
            assert_eq!(vault.get_collateral(acc.bob), 2_000);
            assert_eq!(vault.get_accumulated_fees(), 500);

            // Replay is refused permanently.
            set_caller(acc.django);
            assert_eq!(
                vault.vote_slash(acc.bob, swap_ref, 3_000, acc.charlie, 2_500),
                Err(Error::AlreadySlashed)
            );
        }

        /// Anti-grief: one validator voting a junk verdict must not be able to
        /// park a swap_ref's round for a whole TTL, renewably, and stall the
        /// real reimbursement. Divergent figures open SEPARATE rounds.
        #[ink::test]
        fn divergent_slash_verdicts_open_separate_rounds() {
            let acc = accounts();
            let mut vault =
                seeded_vault_at(ink::prelude::vec![acc.django, acc.eve, acc.charlie], 66);
            post(&mut vault, acc.bob, 5_000);

            // The griefer's verdict lands first, on the same swap_ref.
            let swap_ref = Hash::from([9u8; 32]);
            set_caller(acc.charlie);
            vault
                .vote_slash(acc.bob, swap_ref, 5_000, acc.charlie, 5_000)
                .unwrap();

            // The honest majority's own round is unobstructed by it.
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault
                    .vote_slash(acc.bob, swap_ref, 3_000, acc.frank, 2_500)
                    .unwrap();
            }
            assert!(vault.is_slashed(swap_ref));
            assert_eq!(vault.get_collateral(acc.bob), 2_000);
            assert_eq!(vault.get_accumulated_fees(), 500);

            // And the marker, not the round key, is what stops a second
            // application — the griefer's round can never be completed.
            set_caller(acc.django);
            assert_eq!(
                vault.vote_slash(acc.bob, swap_ref, 5_000, acc.charlie, 5_000),
                Err(Error::AlreadySlashed)
            );
        }

        /// Every figure is still hash-bound: validators who relay different
        /// numbers do not co-count toward one quorum.
        #[ink::test]
        fn slash_round_hash_binds_all_args() {
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);

            let swap_ref = Hash::from([9u8; 32]);
            set_caller(acc.django);
            vault
                .vote_slash(acc.bob, swap_ref, 3_000, acc.charlie, 2_500)
                .unwrap();

            // Same swap_ref, different amount ⇒ its own round, not a vote on
            // django's: at quorum 2 neither applies.
            set_caller(acc.eve);
            vault
                .vote_slash(acc.bob, swap_ref, 2_000, acc.charlie, 1_500)
                .unwrap();
            assert!(!vault.is_slashed(swap_ref));
            assert_eq!(vault.get_collateral(acc.bob), 5_000);

            // Agreeing on django's exact verdict is what applies it.
            vault
                .vote_slash(acc.bob, swap_ref, 3_000, acc.charlie, 2_500)
                .unwrap();
            assert!(vault.is_slashed(swap_ref));
            assert_eq!(vault.get_collateral(acc.bob), 2_000);
        }

        #[ink::test]
        fn seize_clamps_to_collateral() {
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 1_500);

            let swap_ref = Hash::from([3u8; 32]);
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault
                    .vote_slash(acc.bob, swap_ref, 3_000, acc.charlie, 3_000)
                    .unwrap();
            }
            // Only 1_500 existed: all of it reimburses, no surplus.
            assert_eq!(vault.get_collateral(acc.bob), 0);
            assert_eq!(vault.get_accumulated_fees(), 0);
        }

        #[ink::test]
        fn batch_settle_applies_deltas_and_replay_noops() {
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);
            post(&mut vault, acc.charlie, 4_000);

            let batch = ink::prelude::vec![(acc.bob, 300u128), (acc.charlie, 200u128)];
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault.vote_collect_fees_batch(batch.clone()).unwrap();
            }
            assert_eq!(vault.get_collateral(acc.bob), 4_700);
            assert_eq!(vault.get_collateral(acc.charlie), 3_800);
            assert_eq!(vault.get_settled_total(acc.bob), 300);
            assert_eq!(vault.get_accumulated_fees(), 500);

            // Full-batch replay reaches quorum again but every entry is stale
            // against the monotonic totals — books unchanged.
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault.vote_collect_fees_batch(batch.clone()).unwrap();
            }
            assert_eq!(vault.get_collateral(acc.bob), 4_700);
            assert_eq!(vault.get_accumulated_fees(), 500);

            // Next cadence: only the delta beyond the cumulative is collected.
            let next = ink::prelude::vec![(acc.bob, 450u128)];
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault.vote_collect_fees_batch(next.clone()).unwrap();
            }
            assert_eq!(vault.get_collateral(acc.bob), 4_550);
            assert_eq!(vault.get_settled_total(acc.bob), 450);
            assert_eq!(vault.get_accumulated_fees(), 650);
        }

        #[ink::test]
        fn batch_settle_clamps_to_collateral_and_reports_shortfall() {
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 1_000);

            // Owed 1_500 but only 1_000 remains (bond eaten): collect 1_000,
            // write off 500 — and the cumulative still advances to 1_500 so
            // the shortfall is never re-billed on a later round.
            let batch = ink::prelude::vec![(acc.bob, 1_500u128)];
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault.vote_collect_fees_batch(batch.clone()).unwrap();
            }
            assert_eq!(vault.get_collateral(acc.bob), 0);
            assert_eq!(vault.get_accumulated_fees(), 1_000);
            assert_eq!(vault.get_settled_total(acc.bob), 1_500);
        }

        #[ink::test]
        fn batch_rounds_are_keyed_by_contents_hash() {
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);
            post(&mut vault, acc.charlie, 4_000);

            // A cadence batch and an exit's one-entry batch run concurrently:
            // different contents ⇒ different round keys ⇒ no PendingConflict.
            let cadence = ink::prelude::vec![(acc.bob, 300u128), (acc.charlie, 200u128)];
            let exit = ink::prelude::vec![(acc.bob, 350u128)];
            set_caller(acc.django);
            vault.vote_collect_fees_batch(cadence.clone()).unwrap();
            vault.vote_collect_fees_batch(exit.clone()).unwrap();
            // Same validator re-voting the same batch is refused.
            assert_eq!(
                vault.vote_collect_fees_batch(cadence.clone()),
                Err(Error::AlreadyVoted)
            );

            // Each round completes independently.
            set_caller(acc.eve);
            vault.vote_collect_fees_batch(exit).unwrap();
            assert_eq!(vault.get_settled_total(acc.bob), 350);
            vault.vote_collect_fees_batch(cadence).unwrap();
            // Cadence entry (300) is now stale vs 350: no double-collect.
            assert_eq!(vault.get_settled_total(acc.bob), 350);
            assert_eq!(vault.get_collateral(acc.bob), 4_650);
            assert_eq!(vault.get_settled_total(acc.charlie), 200);
            assert_eq!(vault.get_accumulated_fees(), 550);
        }

        #[ink::test]
        fn batch_settle_validates_batch_size() {
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);

            set_caller(acc.django);
            assert_eq!(
                vault.vote_collect_fees_batch(ink::prelude::vec![]),
                Err(Error::InvalidBatch)
            );
            let oversize: Vec<_> = (0..=MAX_BATCH as u64)
                .map(|_| (acc.bob, 1u128))
                .collect();
            assert_eq!(
                vault.vote_collect_fees_batch(oversize),
                Err(Error::InvalidBatch)
            );

            let batch = ink::prelude::vec![(acc.bob, 100u128)];
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault.vote_collect_fees_batch(batch.clone()).unwrap();
            }
            assert_eq!(vault.get_settled_total(acc.bob), 100);
        }

        // ── governance ───────────────────────────────────────────────────────

        #[ink::test]
        fn constructor_refuses_a_set_that_would_brick_the_vault() {
            let acc = accounts();
            set_caller(acc.alice);
            // Empty: miners could bond and lock with nobody able to unlock them.
            assert_eq!(
                AllwaysBondVault::new(acc.frank, 7, MIN_COLLATERAL, 0, 100, 100, Vec::new())
                    .map(|_| ()),
                Err(Error::InvalidValidatorSet)
            );
            // Duplicate: inflates the denominator past what the signers can reach.
            let dupes = ink::prelude::vec![acc.django, acc.eve, acc.django];
            assert_eq!(
                AllwaysBondVault::new(acc.frank, 7, MIN_COLLATERAL, 0, 100, 100, dupes).map(|_| ()),
                Err(Error::InvalidValidatorSet)
            );
            // A TTL below the floor could leave rounds unable to gather votes.
            let one = ink::prelude::vec![acc.django];
            assert_eq!(
                AllwaysBondVault::new(acc.frank, 7, MIN_COLLATERAL, 0, 100, 1, one.clone())
                    .map(|_| ()),
                Err(Error::InvalidAmount)
            );
            // A sub-majority threshold would let a minority quorum fabricate a
            // slash against any bond, so it never reaches deployment.
            assert_eq!(
                AllwaysBondVault::new(
                    acc.frank,
                    7,
                    MIN_COLLATERAL,
                    0,
                    MIN_THRESHOLD - 1,
                    100,
                    one.clone()
                )
                .map(|_| ()),
                Err(Error::ThresholdTooLow)
            );
            assert!(AllwaysBondVault::new(
                acc.frank,
                7,
                MIN_COLLATERAL,
                0,
                MIN_THRESHOLD,
                100,
                one.clone()
            )
            .is_ok());
            assert!(
                AllwaysBondVault::new(acc.frank, 7, MIN_COLLATERAL, 0, 100, 100, one).is_ok()
            );
        }

        /// The floor's whole point: ceil(n*51/100) must exceed n/2 at every set
        /// size the vault can reach, so a minority can never carry a slash.
        #[ink::test]
        fn the_threshold_floor_is_a_strict_majority_at_every_set_size() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django]);
            vault.consensus_threshold_percent = MIN_THRESHOLD;
            for n in 1..=MAX_VALIDATORS {
                vault.validators = (0..n as u8)
                    .map(|i| AccountId::from([i; 32]))
                    .collect();
                assert!(u64::from(vault.get_required_votes()) * 2 > n as u64);
            }
        }

        /// The bootstrap ramp: a lone seed validator adds the second alone,
        /// after which both are required.
        #[ink::test]
        fn ramp_from_one_validator_needs_unanimity_at_every_step() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django]);

            set_caller(acc.django);
            vault.vote_add_validator(acc.eve).unwrap();
            // Approved but NOT yet a validator — eve must prove the key first.
            assert_eq!(vault.get_validators(), ink::prelude::vec![acc.django]);
            assert_eq!(vault.get_pending_validators(), ink::prelude::vec![acc.eve]);
            set_caller(acc.eve);
            vault.accept_validator().unwrap();
            assert_eq!(vault.get_validators(), ink::prelude::vec![acc.django, acc.eve]);

            // At n=2 one vote is no longer enough to add a third.
            set_caller(acc.django);
            vault.vote_add_validator(acc.charlie).unwrap();
            assert!(vault.get_pending_validators().is_empty());
            set_caller(acc.eve);
            vault.vote_add_validator(acc.charlie).unwrap();
            assert_eq!(vault.get_pending_validators(), ink::prelude::vec![acc.charlie]);
        }

        #[ink::test]
        fn accept_is_required_and_only_by_the_candidate() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django]);
            set_caller(acc.django);
            vault.vote_add_validator(acc.eve).unwrap();

            // A bystander cannot claim someone else's admission.
            set_caller(acc.charlie);
            assert_eq!(vault.accept_validator(), Err(Error::NotPendingValidator));
            // Nor can a pending candidate vote before accepting.
            set_caller(acc.eve);
            assert_eq!(vault.vote_add_validator(acc.charlie), Err(Error::NotValidator));
            vault.accept_validator().unwrap();
            assert_eq!(vault.accept_validator(), Err(Error::NotPendingValidator));
        }

        /// The bootstrap-freeze risk: a candidate approved by a one-validator
        /// seed set, who stalls and surfaces later, would otherwise join a set
        /// that never agreed to them — and every money quorum would then need
        /// the sleeper's signature.
        #[ink::test]
        fn an_expired_admission_can_never_accept() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django]);
            set_caller(acc.django);
            vault.vote_add_validator(acc.eve).unwrap();
            assert_eq!(vault.get_pending_validators(), ink::prelude::vec![acc.eve]);

            advance_blocks(vault.get_vote_round_ttl() + 1);
            assert!(vault.get_pending_validators().is_empty());
            set_caller(acc.eve);
            assert_eq!(vault.accept_validator(), Err(Error::AdmissionVoid));
            assert_eq!(vault.get_validators(), ink::prelude::vec![acc.django]);
        }

        /// An approval is consent from THAT set. Change the set and the consent
        /// is gone — the new set must approve the candidate itself.
        #[ink::test]
        fn a_set_change_voids_a_pending_admission() {
            let acc = accounts();
            let mut vault =
                seeded_vault(ink::prelude::vec![acc.django, acc.eve, acc.charlie]);
            for validator in [acc.django, acc.eve, acc.charlie] {
                set_caller(validator);
                vault.vote_add_validator(acc.frank).unwrap();
            }
            assert_eq!(vault.get_pending_validators(), ink::prelude::vec![acc.frank]);

            // The set frank was approved by no longer exists.
            set_caller(acc.django);
            vault.vote_remove_validator(acc.charlie).unwrap();
            set_caller(acc.eve);
            vault.vote_remove_validator(acc.charlie).unwrap();

            assert!(vault.get_pending_validators().is_empty());
            set_caller(acc.frank);
            assert_eq!(vault.accept_validator(), Err(Error::AdmissionVoid));
            assert_eq!(vault.get_validators(), ink::prelude::vec![acc.django, acc.eve]);
        }

        /// Dead admissions must not sit on the MAX_VALIDATORS budget forever —
        /// otherwise a full pending list permanently blocks every real add.
        #[ink::test]
        fn expired_admissions_are_swept_and_their_slots_reclaimed() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django]);
            set_caller(acc.django);
            // Fill every remaining slot with approvals nobody ever accepts.
            for i in 0..MAX_VALIDATORS as u8 - 1 {
                vault
                    .vote_add_validator(AccountId::from([100 + i; 32]))
                    .unwrap();
            }
            assert_eq!(
                vault.vote_add_validator(acc.eve),
                Err(Error::InvalidValidatorSet)
            );

            advance_blocks(vault.get_vote_round_ttl() + 1);
            vault.vote_add_validator(acc.eve).unwrap();
            assert_eq!(vault.get_pending_validators(), ink::prelude::vec![acc.eve]);
            // Swept from storage, not merely hidden from the view.
            assert_eq!(vault.pending_validators.len(), 1);
        }

        #[ink::test]
        fn removal_excludes_the_target_and_holds_the_floor() {
            let acc = accounts();
            let mut vault =
                seeded_vault(ink::prelude::vec![acc.django, acc.eve, acc.charlie]);

            // The target may not vote itself out of the tally.
            set_caller(acc.charlie);
            assert_eq!(vault.vote_remove_validator(acc.charlie), Err(Error::SelfRemoval));

            // The other two are enough — charlie's consent is not needed.
            set_caller(acc.django);
            vault.vote_remove_validator(acc.charlie).unwrap();
            assert_eq!(vault.get_validators().len(), 3);
            set_caller(acc.eve);
            vault.vote_remove_validator(acc.charlie).unwrap();
            assert_eq!(vault.get_validators(), ink::prelude::vec![acc.django, acc.eve]);

            // At n=2 removal is refused outright — it could strand the set at one.
            set_caller(acc.django);
            assert_eq!(
                vault.vote_remove_validator(acc.eve),
                Err(Error::InvalidValidatorSet)
            );
        }

        /// A membership change must void in-flight governance rounds, or one
        /// opened under a larger set could complete against a smaller one.
        #[ink::test]
        fn membership_rounds_are_bound_to_the_set_they_opened_under() {
            let acc = accounts();
            let mut vault =
                seeded_vault(ink::prelude::vec![acc.django, acc.eve, acc.charlie]);

            set_caller(acc.django);
            vault.vote_add_validator(acc.frank).unwrap();

            // Set changes underneath the open round.
            set_caller(acc.django);
            vault.vote_remove_validator(acc.charlie).unwrap();
            set_caller(acc.eve);
            vault.vote_remove_validator(acc.charlie).unwrap();
            assert_eq!(vault.get_validators().len(), 2);

            // The stale add round no longer matches: it conflicts, not counts.
            set_caller(acc.eve);
            assert_eq!(vault.vote_add_validator(acc.frank), Err(Error::PendingConflict));
        }

        /// Ejecting a validator must neutralise their in-flight votes — not
        /// leave them behind against a now-lower bar.
        #[ink::test]
        fn a_removed_validators_votes_stop_counting() {
            let acc = accounts();
            let mut vault =
                seeded_vault(ink::prelude::vec![acc.django, acc.eve, acc.charlie]);
            post(&mut vault, acc.bob, 5_000);

            // charlie opens a slash round and votes (1 of 2 needed at n=3).
            let swap_ref = Hash::from([21u8; 32]);
            set_caller(acc.charlie);
            vault.vote_slash(acc.bob, swap_ref, 1_000, acc.frank, 0).unwrap();

            set_caller(acc.django);
            vault.vote_remove_validator(acc.charlie).unwrap();
            set_caller(acc.eve);
            vault.vote_remove_validator(acc.charlie).unwrap();
            assert_eq!(vault.get_validators().len(), 2);

            // n=2 needs 2 votes; charlie's stale vote is discounted, so django
            // alone must NOT complete the slash charlie started.
            set_caller(acc.django);
            vault.vote_slash(acc.bob, swap_ref, 1_000, acc.frank, 0).unwrap();
            assert!(!vault.is_slashed(swap_ref));
            assert_eq!(vault.get_collateral(acc.bob), 5_000);
        }

        #[ink::test]
        fn config_round_is_unanimous_and_validated() {
            let acc = accounts();
            let mut vault = new_vault();

            set_caller(acc.django);
            assert_eq!(vault.vote_set_config(1, 2, 101, 600), Err(Error::InvalidAmount));
            // Sub-majority thresholds are refused: at 50 or below, ceil() can
            // hand a minority — at 1%, a single validator — a money quorum.
            assert_eq!(vault.vote_set_config(1, 2, 0, 600), Err(Error::ThresholdTooLow));
            assert_eq!(vault.vote_set_config(1, 2, 1, 600), Err(Error::ThresholdTooLow));
            assert_eq!(
                vault.vote_set_config(1, 2, MIN_THRESHOLD - 1, 600),
                Err(Error::ThresholdTooLow)
            );
            // Below the TTL floor: a unanimous mistake must not be able to
            // starve every future round of the time to gather votes.
            assert_eq!(
                vault.vote_set_config(1, 2, 66, MIN_VOTE_ROUND_TTL - 1),
                Err(Error::InvalidAmount)
            );

            vault.vote_set_config(500, 9_000, 66, 600).unwrap();
            assert_eq!(vault.get_min_collateral(), MIN_COLLATERAL);
            set_caller(acc.eve);
            vault.vote_set_config(500, 9_000, 66, 600).unwrap();
            assert_eq!(vault.get_min_collateral(), 500);
            assert_eq!(vault.get_max_collateral(), 9_000);
            assert_eq!(vault.get_consensus_threshold(), 66);
            assert_eq!(vault.get_vote_round_ttl(), 600);
        }

        #[ink::test]
        fn config_rounds_agree_on_the_whole_tuple_not_a_delta() {
            let acc = accounts();
            let mut vault = new_vault();
            set_caller(acc.django);
            vault.vote_set_config(500, 9_000, 66, 600).unwrap();

            // eve votes a DIFFERENT resulting config. Rounds are keyed by the
            // contents hash, so this opens its own round rather than joining
            // django's — divergent proposals never merge, and neither reaches
            // unanimity. (Keying by contents also stops one validator parking a
            // junk round that blocks every other proposal until it expires.)
            set_caller(acc.eve);
            vault.vote_set_config(500, 9_000, 51, 600).unwrap();
            assert_eq!(vault.get_min_collateral(), MIN_COLLATERAL);
            assert_eq!(vault.get_consensus_threshold(), 100);

            // Agreeing on django's exact tuple is what applies it.
            vault.vote_set_config(500, 9_000, 66, 600).unwrap();
            assert_eq!(vault.get_min_collateral(), 500);
            assert_eq!(vault.get_consensus_threshold(), 66);
        }

        /// The recycle destination is the one config the vault cannot survive
        /// going stale: an unstakeable hotkey reverts every recycle forever.
        #[ink::test]
        fn recycle_target_moves_only_by_a_unanimous_round() {
            let acc = accounts();
            let mut vault = new_vault();
            assert_eq!(vault.get_staking_hotkey(), acc.frank);
            assert_eq!(vault.get_netuid(), 7);

            set_caller(acc.alice);
            assert_eq!(
                vault.vote_set_recycle_target(acc.charlie, 9),
                Err(Error::NotValidator)
            );

            // One of two validators is not unanimity — nothing moves.
            set_caller(acc.django);
            vault.vote_set_recycle_target(acc.charlie, 9).unwrap();
            assert_eq!(vault.get_staking_hotkey(), acc.frank);
            assert_eq!(vault.get_netuid(), 7);

            // A divergent target opens its own round rather than joining.
            set_caller(acc.eve);
            vault.vote_set_recycle_target(acc.charlie, 11).unwrap();
            assert_eq!(vault.get_staking_hotkey(), acc.frank);

            // Agreeing on the exact pair applies both fields at once.
            vault.vote_set_recycle_target(acc.charlie, 9).unwrap();
            assert_eq!(vault.get_staking_hotkey(), acc.charlie);
            assert_eq!(vault.get_netuid(), 9);
        }

        /// Bound to the approving set exactly like `vote_set_config`: a
        /// membership change must void an in-flight target round.
        #[ink::test]
        fn a_set_change_voids_an_open_recycle_target_round() {
            let acc = accounts();
            let mut vault =
                seeded_vault(ink::prelude::vec![acc.django, acc.eve, acc.charlie]);

            set_caller(acc.django);
            vault.vote_set_recycle_target(acc.bob, 9).unwrap();

            set_caller(acc.django);
            vault.vote_remove_validator(acc.charlie).unwrap();
            set_caller(acc.eve);
            vault.vote_remove_validator(acc.charlie).unwrap();

            // django's pre-change vote is stranded under the old set's round:
            // at n=2 eve's vote alone is not unanimity, so nothing applies.
            set_caller(acc.eve);
            vault.vote_set_recycle_target(acc.bob, 9).unwrap();
            assert_eq!(vault.get_staking_hotkey(), acc.frank);

            // The surviving set votes it through under its own hash.
            set_caller(acc.django);
            vault.vote_set_recycle_target(acc.bob, 9).unwrap();
            assert_eq!(vault.get_staking_hotkey(), acc.bob);
        }

        #[ink::test]
        fn non_validators_cannot_touch_governance() {
            let acc = accounts();
            let mut vault = new_vault();
            set_caller(acc.alice); // the deployer — no longer special in any way
            assert_eq!(vault.vote_add_validator(acc.frank), Err(Error::NotValidator));
            assert_eq!(vault.vote_remove_validator(acc.eve), Err(Error::NotValidator));
            assert_eq!(vault.vote_set_config(1, 2, 66, 600), Err(Error::NotValidator));
        }

        #[ink::test]
        fn duplicate_admission_is_refused() {
            let acc = accounts();
            let mut vault = seeded_vault(ink::prelude::vec![acc.django]);
            set_caller(acc.django);
            assert_eq!(vault.vote_add_validator(acc.django), Err(Error::AlreadyValidator));
            vault.vote_add_validator(acc.eve).unwrap();
            assert_eq!(vault.vote_add_validator(acc.eve), Err(Error::AlreadyValidator));
        }

        /// Mock of the subtensor extension so recycle can run offchain.
        struct MockRecycleExt;
        impl ink::env::test::ChainExtension for MockRecycleExt {
            fn ext_id(&self) -> u16 {
                0x1000
            }
            /// Cannot debit the contract here — re-entering the test engine
            /// from inside an extension panics on its RefCell — so tests that
            /// care about the post-recycle balance call `debit_contract`.
            fn call(&mut self, _func_id: u16, _input: &[u8], output: &mut Vec<u8>) -> u32 {
                scale::Encode::encode_to(&0u64, output);
                0
            }
        }

        #[ink::test]
        fn recycle_drains_pot_via_chain_ext() {
            ink::env::test::register_chain_extension(MockRecycleExt);
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);

            // Slash with zero reimbursement fills the pot.
            let swap_ref = Hash::from([5u8; 32]);
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault
                    .vote_slash(acc.bob, swap_ref, 1_000, acc.charlie, 0)
                    .unwrap();
            }
            assert_eq!(vault.get_accumulated_fees(), 1_000);

            set_caller(acc.charlie);
            vault.recycle_fees().unwrap();
            assert_eq!(vault.get_accumulated_fees(), 0);
            assert_eq!(vault.get_total_recycled_fees(), 1_000);
            // Empty pot refuses a second drain.
            debit_contract(1_000);
            assert_eq!(vault.recycle_fees(), Err(Error::InvalidAmount));
        }

        /// The property the whole donation design rests on: a pot derived from
        /// the real balance must never be able to reach money that is owed.
        #[ink::test]
        fn recycle_can_never_reach_collateral_or_pending_claims() {
            ink::env::test::register_chain_extension(MockRecycleExt);
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);
            post(&mut vault, acc.charlie, 4_000);

            // Bonds only, no fees and no donations ⇒ nothing is recyclable.
            assert_eq!(vault.get_recyclable_pot(), 0);
            assert_eq!(vault.recycle_fees(), Err(Error::InvalidAmount));

            // A fully-reimbursed slash pays the user out of the balance and
            // leaves no surplus, so the pot stays shut even as bonds move.
            let swap_ref = Hash::from([11u8; 32]);
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault
                    .vote_slash(acc.bob, swap_ref, 1_000, acc.frank, 1_000)
                    .unwrap();
            }
            assert_eq!(vault.get_total_collateral(), 8_000);
            assert_eq!(vault.get_recyclable_pot(), 0);
            assert_eq!(vault.recycle_fees(), Err(Error::InvalidAmount));
        }

        #[ink::test]
        fn plain_transfer_to_the_address_is_swept_into_the_next_recycle() {
            ink::env::test::register_chain_extension(MockRecycleExt);
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);

            // 2 TAO arrives as a bare transfer — no code ran, no event, and the
            // fee counter knows nothing about it.
            donate_raw(2_000);
            assert_eq!(vault.get_accumulated_fees(), 0);
            assert_eq!(vault.get_recyclable_pot(), 2_000);

            set_caller(acc.charlie);
            vault.recycle_fees().unwrap();
            assert_eq!(vault.get_total_recycled_fees(), 2_000);
            debit_contract(2_000);
            assert_eq!(vault.get_recyclable_pot(), 0);
            // The bond it was commingled with is untouched.
            assert_eq!(vault.get_collateral(acc.bob), 5_000);
            assert_eq!(vault.get_total_collateral(), 5_000);
        }

        #[ink::test]
        fn donations_and_fees_recycle_together() {
            ink::env::test::register_chain_extension(MockRecycleExt);
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);

            let batch = ink::prelude::vec![(acc.bob, 300u128)];
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault.vote_collect_fees_batch(batch.clone()).unwrap();
            }
            assert_eq!(vault.get_accumulated_fees(), 300);

            set_caller(acc.charlie);
            donate_raw(700);

            // 300 settled fees + 700 donated drain as one pot.
            assert_eq!(vault.get_recyclable_pot(), 1_000);
            vault.recycle_fees().unwrap();
            assert_eq!(vault.get_accumulated_fees(), 0);
            assert_eq!(vault.get_total_recycled_fees(), 1_000);
            assert_eq!(vault.get_collateral(acc.bob), 4_700);
        }


        /// `total_collateral` is what stands between a sweep and miner funds,
        /// so it must equal the mapping through every debit path.
        #[ink::test]
        fn total_collateral_tracks_the_mapping_through_every_path() {
            let acc = accounts();
            let mut vault = new_vault();
            post(&mut vault, acc.bob, 5_000);
            post(&mut vault, acc.charlie, 4_000);
            post(&mut vault, acc.bob, 1_000);

            let swap_ref = Hash::from([13u8; 32]);
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault
                    .vote_slash(acc.bob, swap_ref, 2_000, acc.eve, 1_500)
                    .unwrap();
            }
            let batch = ink::prelude::vec![(acc.charlie, 400u128)];
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault.vote_collect_fees_batch(batch.clone()).unwrap();
            }
            set_caller(acc.charlie);
            vault.withdraw_collateral(600).unwrap();

            let summed = vault.get_collateral(acc.bob) + vault.get_collateral(acc.charlie);
            assert_eq!(vault.get_total_collateral(), summed);
            // And the invariant the design record states, in code.
            assert!(
                contract_balance()
                    >= vault.get_total_collateral()
                        + vault.get_accumulated_fees()
                        + vault.get_pending_slash_total()
            );
        }

    }
}

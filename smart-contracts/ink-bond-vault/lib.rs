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
    use ink::prelude::string::String;
    use ink::prelude::vec::Vec;
    use ink::storage::Mapping;

    // Round-type discriminants, bound into every request hash so unlock,
    // slash, and fee-settle rounds can never collide.
    const REQ_UNLOCK: u8 = 0;
    const REQ_SLASH: u8 = 1;
    const REQ_COLLECT: u8 = 2;

    // Fee-settle batch ceiling: bounds per-entry storage writes so a quorum
    // application always fits a block. One cadence round covers ≤256 miners;
    // larger fleets split into multiple batches.
    const MAX_BATCH: usize = 256;

    #[ink(storage)]
    pub struct AllwaysBondVault {
        // Configuration. staking_hotkey + netuid are the hard-coded
        // add_stake_recycle target — there is deliberately NO custodial
        // recycle_address fallback in this contract (the pot is ownerless).
        // No chain-ext latch either: #2560 verified live in runtime v443.
        owner: AccountId,
        staking_hotkey: AccountId,
        netuid: u16,
        halted: bool,
        min_collateral: Balance,
        max_collateral: Balance,
        consensus_threshold_percent: u8,
        // Blocks before an unfinished vote round expires and can be replaced.
        vote_round_ttl: u32,
        // Whitelisted validator set — Vec for enumeration, tiny by policy.
        validators: Vec<AccountId>,

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
        // One live unlock round per miner; one live slash round per swap_ref
        // (keyed by swap_ref, not miner, so two timed-out swaps for the same
        // miner can be slashed concurrently).
        unlock_request: Mapping<AccountId, u64>,
        slash_request: Mapping<Hash, u64>,
        // Fee-settle rounds keyed by the batch-contents hash, so a cadence
        // batch and an exit's one-entry batch can run concurrently.
        collect_request: Mapping<Hash, u64>,

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
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    impl AllwaysBondVault {
        fn ensure_owner(&self) -> Result<(), Error> {
            if self.env().caller() != self.owner {
                return Err(Error::NotOwner);
            }
            Ok(())
        }

        fn ensure_validator(&self) -> Result<(), Error> {
            if !self.validators.contains(&self.env().caller()) {
                return Err(Error::NotValidator);
            }
            Ok(())
        }

        fn ensure_not_halted(&self) -> Result<(), Error> {
            if self.halted {
                return Err(Error::SystemHalted);
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

        /// AccountId → Hash (both 32 bytes) so `VaultVoteCast.subject` can
        /// carry a miner or a swap_ref through one field.
        fn account_hash(account: &AccountId) -> Hash {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(account.as_ref());
            Hash::from(bytes)
        }

        fn record_vote(&mut self, request_id: u64, caller: AccountId) -> Result<u32, Error> {
            let mut voters = self.request_voters.get(request_id).unwrap_or_default();
            if voters.contains(&caller) {
                return Err(Error::AlreadyVoted);
            }
            voters.push(caller);
            let count = u32::try_from(voters.len()).unwrap_or(u32::MAX);
            self.request_voters.insert(request_id, &voters);
            Ok(count)
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
            Ok((id, votes, votes >= self.get_required_votes()))
        }
    }

    impl AllwaysBondVault {
        #[ink(constructor)]
        pub fn new(
            staking_hotkey: AccountId,
            netuid: u16,
            min_collateral: Balance,
            max_collateral: Balance,
            consensus_threshold_percent: u8,
            vote_round_ttl: u32,
        ) -> Self {
            Self {
                owner: Self::env().caller(),
                staking_hotkey,
                netuid,
                halted: false,
                min_collateral,
                max_collateral,
                consensus_threshold_percent,
                vote_round_ttl,
                validators: Vec::new(),

                collateral: Mapping::default(),
                lock_state: Mapping::default(),

                next_request_id: 1,
                request_voters: Mapping::default(),
                request_created: Mapping::default(),
                request_hash: Mapping::default(),
                unlock_request: Mapping::default(),
                slash_request: Mapping::default(),
                collect_request: Mapping::default(),
                settled_total: Mapping::default(),

                slashed: Mapping::default(),
                pending_slashes: Mapping::default(),

                accumulated_fees: 0,
                total_recycled_fees: 0,
            }
        }

        // =====================================================================
        // Bond management (miner direct — caller-based auth)
        // =====================================================================

        #[ink(message, payable)]
        pub fn post_collateral(&mut self) -> Result<(), Error> {
            self.ensure_not_halted()?;
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
        /// Solana as "eligible for vote_activate".
        #[ink(message)]
        pub fn lock_bond(&mut self) -> Result<(), Error> {
            self.ensure_not_halted()?;
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
        /// validators must relay the identical verdict. On quorum: seize
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
            let existing = self.slash_request.get(swap_ref);
            let (id, votes, quorum) = self.cast_vote(existing, round_hash)?;
            self.slash_request.insert(swap_ref, &id);

            self.env().emit_event(VaultVoteCast {
                validator: self.env().caller(),
                req_type: REQ_SLASH,
                request_id: id,
                subject: swap_ref,
                vote_count: votes,
            });

            if quorum {
                // Permanent marker first: this swap_ref can never slash again.
                self.slashed.insert(swap_ref, &true);
                self.clear_request_data(id);
                self.slash_request.remove(swap_ref);

                let current = self.collateral.get(miner).unwrap_or(0);
                let seized = core::cmp::min(penalty, current);
                let reimbursed = core::cmp::min(reimbursement, seized);
                let surplus = seized.saturating_sub(reimbursed);

                self.collateral
                    .insert(miner, &current.saturating_sub(seized));
                self.accumulated_fees = self.accumulated_fees.saturating_add(surplus);

                if reimbursed > 0 && self.env().transfer(user, reimbursed).is_err() {
                    self.pending_slashes
                        .insert(swap_ref, &(user, reimbursed));
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
            self.env().transfer(caller, amount).map_err(|_| {
                self.pending_slashes.insert(swap_ref, &(user, amount));
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

        /// Permissionless, caller-pays. Drains the whole pot into
        /// add_stake_recycle(staking_hotkey, netuid) — there is no other
        /// destination and no owner path to the funds.
        #[ink(message)]
        pub fn recycle_fees(&mut self) -> Result<(), Error> {
            let fees = self.accumulated_fees;
            if fees == 0 {
                return Err(Error::InvalidAmount);
            }

            // try_into is a no-op on-chain (Balance = u64) but real under the
            // test env's u128 Balance — keep it despite clippy.
            #[allow(clippy::useless_conversion)]
            let amount: u64 = fees.try_into().map_err(|_| Error::TransferFailed)?;
            self.env()
                .extension()
                .add_stake_recycle(self.staking_hotkey, self.netuid, amount)
                .map_err(|SubtensorError::Code(c)| Error::ChainExtension(c))?;

            self.accumulated_fees = 0;
            self.total_recycled_fees = self.total_recycled_fees.saturating_add(fees);
            self.env().emit_event(FeesRecycled { tao_amount: fees.into() });
            Ok(())
        }

        // =====================================================================
        // Owner configuration (validator-set/config admin only — no fund paths)
        // =====================================================================

        #[ink(message)]
        pub fn transfer_ownership(&mut self, new_owner: AccountId) -> Result<(), Error> {
            self.ensure_owner()?;
            let previous_owner = self.owner;
            self.owner = new_owner;
            self.env()
                .emit_event(OwnershipTransferred { previous_owner, new_owner });
            Ok(())
        }

        #[ink(message)]
        pub fn add_validator(&mut self, validator: AccountId) -> Result<(), Error> {
            self.ensure_owner()?;
            if !self.validators.contains(&validator) {
                self.validators.push(validator);
            }
            self.env().emit_event(ValidatorUpdated {
                validator,
                registered: true,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn remove_validator(&mut self, validator: AccountId) -> Result<(), Error> {
            self.ensure_owner()?;
            self.validators.retain(|v| v != &validator);
            self.env().emit_event(ValidatorUpdated {
                validator,
                registered: false,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_min_collateral(&mut self, amount: Balance) -> Result<(), Error> {
            self.ensure_owner()?;
            self.min_collateral = amount;
            self.env().emit_event(ConfigUpdated {
                key: String::from("min_collateral"),
                value: amount.into(),
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_max_collateral(&mut self, amount: Balance) -> Result<(), Error> {
            self.ensure_owner()?;
            self.max_collateral = amount;
            self.env().emit_event(ConfigUpdated {
                key: String::from("max_collateral"),
                value: amount.into(),
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_consensus_threshold(&mut self, percent: u8) -> Result<(), Error> {
            self.ensure_owner()?;
            if percent == 0 || percent > 100 {
                return Err(Error::InvalidAmount);
            }
            self.consensus_threshold_percent = percent;
            self.env().emit_event(ConfigUpdated {
                key: String::from("consensus_threshold_percent"),
                value: percent as u128,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_vote_round_ttl(&mut self, blocks: u32) -> Result<(), Error> {
            self.ensure_owner()?;
            if blocks == 0 {
                return Err(Error::InvalidAmount);
            }
            self.vote_round_ttl = blocks;
            self.env().emit_event(ConfigUpdated {
                key: String::from("vote_round_ttl"),
                value: blocks as u128,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_halted(&mut self, halted: bool) -> Result<(), Error> {
            self.ensure_owner()?;
            self.halted = halted;
            self.env().emit_event(ConfigUpdated {
                key: String::from("halted"),
                value: halted as u128,
            });
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

        #[ink(message)]
        pub fn get_validators(&self) -> Vec<AccountId> {
            self.validators.clone()
        }

        #[ink(message)]
        pub fn get_owner(&self) -> AccountId {
            self.owner
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
        pub fn get_halted(&self) -> bool {
            self.halted
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

        fn fund_contract(amount: Balance) {
            ink::env::test::set_account_balance::<crate::CustomEnvironment>(
                contract_id(),
                amount,
            );
        }

        /// Vault with alice as owner and (django, eve) as validators,
        /// threshold 100% ⇒ quorum = 2.
        fn new_vault() -> AllwaysBondVault {
            let acc = accounts();
            set_caller(acc.alice);
            let mut vault = AllwaysBondVault::new(acc.frank, 7, MIN_COLLATERAL, 0, 100, 100);
            vault.add_validator(acc.django).unwrap();
            vault.add_validator(acc.eve).unwrap();
            vault
        }

        fn post(vault: &mut AllwaysBondVault, miner: AccountId, amount: Balance) {
            set_caller(miner);
            ink::env::test::set_value_transferred::<crate::CustomEnvironment>(amount);
            vault.post_collateral().unwrap();
            ink::env::test::set_value_transferred::<crate::CustomEnvironment>(0);
        }

        #[ink::test]
        fn lock_blocks_withdraw_until_quorum_unlock() {
            let acc = accounts();
            let mut vault = new_vault();
            fund_contract(1_000_000);
            post(&mut vault, acc.bob, 5_000);

            set_caller(acc.bob);
            vault.lock_bond().unwrap();
            assert_eq!(vault.get_lock_state(acc.bob), (true, 1));
            assert_eq!(vault.withdraw_collateral(1_000), Err(Error::BondLocked));

            // One validator vote isn't quorum; bond stays locked.
            set_caller(acc.django);
            vault.vote_unlock(acc.bob, 1).unwrap();
            assert_eq!(vault.get_lock_state(acc.bob), (true, 1));

            // Second vote reaches quorum: unlocked, epoch bumped.
            set_caller(acc.eve);
            vault.vote_unlock(acc.bob, 1).unwrap();
            assert_eq!(vault.get_lock_state(acc.bob), (false, 2));

            set_caller(acc.bob);
            vault.withdraw_collateral(1_000).unwrap();
            assert_eq!(vault.get_collateral(acc.bob), 4_000);
        }

        #[ink::test]
        fn unlock_vote_requires_current_epoch() {
            let acc = accounts();
            let mut vault = new_vault();
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
            let mut vault = new_vault();
            post(&mut vault, acc.bob, MIN_COLLATERAL - 1);
            set_caller(acc.bob);
            assert_eq!(vault.lock_bond(), Err(Error::InsufficientCollateral));
        }

        #[ink::test]
        fn slash_reimburses_user_and_credits_surplus() {
            let acc = accounts();
            let mut vault = new_vault();
            fund_contract(1_000_000);
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

            // Same swap_ref, different amount ⇒ different hash ⇒ conflict.
            set_caller(acc.eve);
            assert_eq!(
                vault.vote_slash(acc.bob, swap_ref, 2_000, acc.charlie, 1_500),
                Err(Error::PendingConflict)
            );
        }

        #[ink::test]
        fn seize_clamps_to_collateral() {
            let acc = accounts();
            let mut vault = new_vault();
            fund_contract(1_000_000);
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
        fn batch_settle_validates_size_and_ignores_halt() {
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

            // Halt gates entry, never exit: settlement runs while halted.
            set_caller(acc.alice);
            vault.set_halted(true).unwrap();
            let batch = ink::prelude::vec![(acc.bob, 100u128)];
            for validator in [acc.django, acc.eve] {
                set_caller(validator);
                vault.vote_collect_fees_batch(batch.clone()).unwrap();
            }
            assert_eq!(vault.get_settled_total(acc.bob), 100);
        }

        /// Mock of the subtensor extension so recycle can run offchain.
        struct MockRecycleExt;
        impl ink::env::test::ChainExtension for MockRecycleExt {
            fn ext_id(&self) -> u16 {
                0x1000
            }
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
            fund_contract(1_000_000);
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
            assert_eq!(vault.recycle_fees(), Err(Error::InvalidAmount));
        }
    }
}

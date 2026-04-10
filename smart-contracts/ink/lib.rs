#![cfg_attr(not(feature = "std"), no_std, no_main)]

mod types;
mod errors;
mod events;

use types::{SwapData, SwapStatus, VoteType};
use errors::Error;

#[ink::contract]
mod allways_swap_manager {
    use super::*;
    use events::*;
    use ink::codegen::Env;
    use ink::prelude::string::String;
    use ink::prelude::vec::Vec;
    use ink::storage::Mapping;

    #[ink(storage)]
    pub struct AllwaysSwapManager {
        // Configuration
        owner: AccountId,
        recycle_address: AccountId,
        fulfillment_timeout_blocks: u32,
        reservation_ttl: u32,
        min_collateral: Balance,
        max_collateral: Balance,
        min_swap_amount: Balance,
        max_swap_amount: Balance,
        consensus_threshold_percent: u8,
        validator_count: u32,
        fee_divisor: u128,
        halted: bool,
        validators: Mapping<AccountId, bool>,

        // Swap state
        next_swap_id: u64,
        swaps: Mapping<u64, SwapData>,
        swap_confirm_votes: Mapping<(u64, AccountId), bool>,
        swap_confirm_vote_count: Mapping<u64, u32>,
        swap_timeout_votes: Mapping<(u64, AccountId), bool>,
        swap_timeout_vote_count: Mapping<u64, u32>,
        used_source_tx: Mapping<String, bool>,

        // Miner state
        collateral: Mapping<AccountId, Balance>,
        miner_active: Mapping<AccountId, bool>,
        miner_has_active_swap: Mapping<AccountId, bool>,
        miner_reserved_until: Mapping<AccountId, u32>,
        miner_last_resolved_block: Mapping<AccountId, u32>,
        miner_deactivation_block: Mapping<AccountId, u32>,

        // Consensus voting — all vote types use unique request IDs (like swap IDs).
        // Votes keyed by (request_id, validator) so they never conflict across rounds.
        next_request_id: u64,
        request_votes: Mapping<(u64, AccountId), bool>,
        request_vote_count: Mapping<u64, u32>,
        request_created: Mapping<u64, u32>,
        request_hash: Mapping<u64, Hash>,

        // Active request ID per miner per vote type (activation=0, reserve=1, initiate=2)
        miner_active_request: Mapping<(AccountId, u8), u64>,

        // Confirmed reservation data (post-quorum)
        reservation_hash: Mapping<AccountId, Hash>,
        reservation_source_addr: Mapping<AccountId, Vec<u8>>,
        reservation_tao_amount: Mapping<AccountId, Balance>,
        reservation_source_amount: Mapping<AccountId, Balance>,
        reservation_dest_amount: Mapping<AccountId, Balance>,

        // Cooldown strike tracking (lazy eval)
        address_strike_count: Mapping<Vec<u8>, u8>,
        address_last_expired: Mapping<Vec<u8>, u32>,
        // Financials
        accumulated_fees: Balance,
        total_recycled_fees: Balance,
        pending_slashes: Mapping<u64, (AccountId, Balance)>,
    }

    // Request type constants
    const REQ_ACTIVATE: u8 = 0;
    const REQ_RESERVE: u8 = 1;
    const REQ_INITIATE: u8 = 2;
    const REQ_EXTEND: u8 = 3;
    const REQ_EXTEND_TIMEOUT: u8 = 4;

    // Fee cap: divisor >= 20 means fee can never exceed 5%
    const MIN_FEE_DIVISOR: u128 = 20;

    // =========================================================================
    // Internal helpers
    // =========================================================================

    impl AllwaysSwapManager {
        fn ensure_owner(&self) -> Result<(), Error> {
            if self.env().caller() != self.owner {
                return Err(Error::NotOwner);
            }
            Ok(())
        }

        fn ensure_validator(&self) -> Result<(), Error> {
            if !self.validators.get(self.env().caller()).unwrap_or(false) {
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
            if self.validator_count == 0 {
                return 1;
            }
            let numerator = (self.validator_count)
                .saturating_mul(self.consensus_threshold_percent as u32);
            let required = numerator.saturating_add(99) / 100;
            core::cmp::max(1, required)
        }

        fn compute_reserve_hash(
            miner: &AccountId,
            user_source_address: &[u8],
            source_chain: &str,
            dest_chain: &str,
            tao_amount: Balance,
            source_amount: Balance,
            dest_amount: Balance,
        ) -> Hash {
            let mut output = <ink::env::hash::Keccak256 as ink::env::hash::HashOutput>::Type::default();
            ink::env::hash_encoded::<ink::env::hash::Keccak256, _>(
                &(
                    miner,
                    user_source_address,
                    source_chain,
                    dest_chain,
                    tao_amount,
                    source_amount,
                    dest_amount,
                ),
                &mut output,
            );
            Hash::from(output)
        }

        fn compute_initiate_hash(
            miner: &AccountId,
            source_tx_hash: &str,
            source_chain: &str,
            dest_chain: &str,
            miner_source_address: &str,
            miner_dest_address: &str,
            rate: &str,
            tao_amount: Balance,
            source_amount: Balance,
            dest_amount: Balance,
        ) -> Hash {
            let mut output = <ink::env::hash::Keccak256 as ink::env::hash::HashOutput>::Type::default();
            ink::env::hash_encoded::<ink::env::hash::Keccak256, _>(
                &(
                    miner,
                    source_tx_hash,
                    source_chain,
                    dest_chain,
                    miner_source_address,
                    miner_dest_address,
                    rate,
                    tao_amount,
                    source_amount,
                    dest_amount,
                ),
                &mut output,
            );
            Hash::from(output)
        }

        fn compute_extend_hash(
            miner: &AccountId,
            source_tx_hash: &str,
        ) -> Hash {
            let mut output = <ink::env::hash::Keccak256 as ink::env::hash::HashOutput>::Type::default();
            ink::env::hash_encoded::<ink::env::hash::Keccak256, _>(
                &(miner, source_tx_hash),
                &mut output,
            );
            Hash::from(output)
        }

        fn clear_confirmed_reservation(&mut self, miner: AccountId) {
            self.miner_reserved_until.remove(miner);
            self.reservation_hash.remove(miner);
            self.reservation_source_addr.remove(miner);
            self.reservation_tao_amount.remove(miner);
            self.reservation_source_amount.remove(miner);
            self.reservation_dest_amount.remove(miner);
        }

        /// Allocate a new request ID and return it. Also records the miner's active request.
        fn new_request(&mut self, miner: AccountId, req_type: u8, hash: Hash) -> u64 {
            let id = self.next_request_id;
            self.next_request_id = id.saturating_add(1);
            self.request_hash.insert(id, &hash);
            self.request_created.insert(id, &self.env().block_number());
            self.miner_active_request.insert((miner, req_type), &id);
            id
        }

        /// Record a vote on a request. Returns (vote_count, is_new_vote).
        fn record_vote(&mut self, request_id: u64, caller: AccountId) -> Result<u32, Error> {
            if self.request_votes.get((request_id, caller)).unwrap_or(false) {
                return Err(Error::AlreadyVoted);
            }
            self.request_votes.insert((request_id, caller), &true);
            let count = self.request_vote_count.get(request_id).unwrap_or(0).saturating_add(1);
            self.request_vote_count.insert(request_id, &count);
            Ok(count)
        }

        /// Get the active request ID for a miner+type, clearing expired ones.
        fn get_active_request(&mut self, miner: AccountId, req_type: u8) -> Option<u64> {
            let id = self.miner_active_request.get((miner, req_type))?;
            let created = self.request_created.get(id).unwrap_or(0);
            if self.env().block_number() > created.saturating_add(self.reservation_ttl) {
                // Expired — clear it
                self.clear_request_data(id);
                self.miner_active_request.remove((miner, req_type));
                return None;
            }
            Some(id)
        }

        /// Clear a miner's active request for a given type, including request metadata.
        fn clear_request(&mut self, miner: AccountId, req_type: u8) {
            if let Some(id) = self.miner_active_request.get((miner, req_type)) {
                self.clear_request_data(id);
            }
            self.miner_active_request.remove((miner, req_type));
        }

        /// Remove scalar metadata for a completed/expired request.
        fn clear_request_data(&mut self, request_id: u64) {
            self.request_vote_count.remove(request_id);
            self.request_created.remove(request_id);
            self.request_hash.remove(request_id);
        }
    }

    impl AllwaysSwapManager {
        /// Initialize the contract
        #[ink(constructor)]
        pub fn new(
            recycle_address: AccountId,
            fulfillment_timeout_blocks: u32,
            reservation_ttl: u32,
            min_collateral: Balance,
            max_collateral: Balance,
            min_swap_amount: Balance,
            max_swap_amount: Balance,
            consensus_threshold_percent: u8,
        ) -> Self {
            Self {
                owner: Self::env().caller(),
                recycle_address,
                fulfillment_timeout_blocks,
                reservation_ttl,
                min_collateral,
                max_collateral,
                min_swap_amount,
                max_swap_amount,
                consensus_threshold_percent,
                validator_count: 0,
                fee_divisor: 100,
                halted: false,
                validators: Mapping::default(),

                next_swap_id: 1,
                swaps: Mapping::default(),
                swap_confirm_votes: Mapping::default(),
                swap_confirm_vote_count: Mapping::default(),
                swap_timeout_votes: Mapping::default(),
                swap_timeout_vote_count: Mapping::default(),
                used_source_tx: Mapping::default(),

                collateral: Mapping::default(),
                miner_active: Mapping::default(),
                miner_has_active_swap: Mapping::default(),
                miner_reserved_until: Mapping::default(),
                miner_last_resolved_block: Mapping::default(),
                miner_deactivation_block: Mapping::default(),

                next_request_id: 1,
                request_votes: Mapping::default(),
                request_vote_count: Mapping::default(),
                request_created: Mapping::default(),
                request_hash: Mapping::default(),
                miner_active_request: Mapping::default(),

                reservation_hash: Mapping::default(),
                reservation_source_addr: Mapping::default(),
                reservation_tao_amount: Mapping::default(),
                reservation_source_amount: Mapping::default(),
                reservation_dest_amount: Mapping::default(),

                address_strike_count: Mapping::default(),
                address_last_expired: Mapping::default(),
                accumulated_fees: 0,
                total_recycled_fees: 0,
                pending_slashes: Mapping::default(),
            }
        }

        // =====================================================================
        // Collateral Management (Miner direct — caller-based auth)
        // =====================================================================

        #[ink(message, payable)]
        pub fn post_collateral(&mut self) -> Result<(), Error> {
            self.ensure_not_halted()?;
            let caller = self.env().caller();
            let amount = self.env().transferred_value();
            if amount == 0 {
                return Err(Error::ZeroAmount);
            }

            let current = self.collateral.get(caller).unwrap_or(0);
            let new_total = current.saturating_add(amount);
            if self.max_collateral > 0 && new_total > self.max_collateral {
                return Err(Error::ExceedsMaxCollateral);
            }
            self.collateral.insert(caller, &new_total);

            self.env().emit_event(CollateralPosted {
                miner: caller,
                amount,
                total: new_total,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn withdraw_collateral(&mut self, amount: Balance) -> Result<(), Error> {
            let caller = self.env().caller();
            if amount == 0 {
                return Err(Error::ZeroAmount);
            }
            if self.miner_active.get(caller).unwrap_or(false) {
                return Err(Error::MinerStillActive);
            }

            let deactivation_block = self.miner_deactivation_block.get(caller).unwrap_or(0);
            if deactivation_block > 0 {
                let required_wait = self.fulfillment_timeout_blocks.saturating_mul(2);
                if self.env().block_number() < deactivation_block.saturating_add(required_wait) {
                    return Err(Error::WithdrawalCooldown);
                }
            }

            let reserved_until = self.miner_reserved_until.get(caller).unwrap_or(0);
            if reserved_until >= self.env().block_number() {
                return Err(Error::MinerReserved);
            }

            if self.miner_has_active_swap.get(caller).unwrap_or(false) {
                return Err(Error::MinerHasActiveSwap);
            }

            let current = self.collateral.get(caller).unwrap_or(0);
            if amount > current {
                return Err(Error::InsufficientCollateral);
            }

            let remaining = current.saturating_sub(amount);
            self.collateral.insert(caller, &remaining);
            self.env().transfer(caller, amount).map_err(|_| Error::TransferFailed)?;

            self.env().emit_event(CollateralWithdrawn {
                miner: caller,
                amount,
                remaining,
            });
            Ok(())
        }

        // =====================================================================
        // Reservation
        // =====================================================================

        #[ink(message)]
        pub fn vote_reserve(
            &mut self,
            request_hash: Hash,
            miner: AccountId,
            user_source_address: Vec<u8>,
            source_chain: String,
            dest_chain: String,
            tao_amount: Balance,
            source_amount: Balance,
            dest_amount: Balance,
        ) -> Result<(), Error> {
            self.ensure_validator()?;
            self.ensure_not_halted()?;
            let caller = self.env().caller();
            let current_block = self.env().block_number();

            // Verify hash — source_chain and dest_chain are included in the hash,
            // so validators must agree on the direction. No separate check needed.
            let computed = Self::compute_reserve_hash(
                &miner,
                &user_source_address,
                &source_chain,
                &dest_chain,
                tao_amount,
                source_amount,
                dest_amount,
            );
            if computed != request_hash {
                return Err(Error::HashMismatch);
            }

            // Swap amount bounds
            if self.min_swap_amount > 0 && tao_amount < self.min_swap_amount {
                return Err(Error::AmountBelowMinimum);
            }
            if self.max_swap_amount > 0 && tao_amount > self.max_swap_amount {
                return Err(Error::AmountAboveMaximum);
            }

            // Miner must be eligible
            if !self.miner_active.get(miner).unwrap_or(false) {
                return Err(Error::MinerNotActive);
            }
            if self.miner_has_active_swap.get(miner).unwrap_or(false) {
                return Err(Error::MinerHasActiveSwap);
            }
            let miner_collateral = self.collateral.get(miner).unwrap_or(0);
            if self.min_collateral > 0 && miner_collateral < self.min_collateral {
                return Err(Error::InsufficientCollateral);
            }

            // Check confirmed reservation
            let reserved_until = self.miner_reserved_until.get(miner).unwrap_or(0);
            if reserved_until >= current_block {
                return Err(Error::MinerReserved);
            }
            // Lazy strike: expired confirmed reservation -> record strike
            if reserved_until > 0 {
                if let Some(expired_addr) = self.reservation_source_addr.get(miner) {
                    let strikes = self.address_strike_count.get(&expired_addr).unwrap_or(0);
                    self.address_strike_count.insert(&expired_addr, &strikes.saturating_add(1));
                    self.address_last_expired.insert(&expired_addr, &current_block);
                }
                self.clear_confirmed_reservation(miner);
                self.env().emit_event(ReservationCancelled { miner });
            }

            // Get or create request ID for this reserve round
            let request_id = match self.get_active_request(miner, REQ_RESERVE) {
                Some(id) => {
                    // Existing round — verify same hash
                    if self.request_hash.get(id).unwrap_or_default() != request_hash {
                        return Err(Error::PendingConflict);
                    }
                    id
                }
                None => self.new_request(miner, REQ_RESERVE, request_hash),
            };

            // Record vote (rejects duplicates via request_id keying)
            let vote_count = self.record_vote(request_id, caller)?;

            // Check quorum
            if vote_count >= self.get_required_votes() {
                let new_reserved_until = current_block.saturating_add(self.reservation_ttl);
                self.miner_reserved_until.insert(miner, &new_reserved_until);
                self.reservation_hash.insert(miner, &request_hash);
                self.reservation_source_addr.insert(miner, &user_source_address);
                self.reservation_tao_amount.insert(miner, &tao_amount);
                self.reservation_source_amount.insert(miner, &source_amount);
                self.reservation_dest_amount.insert(miner, &dest_amount);
                self.clear_request(miner, REQ_RESERVE);

                self.env().emit_event(MinerReserved {
                    miner,
                    reserved_until: new_reserved_until,
                });
            }

            Ok(())
        }

        #[ink(message)]
        pub fn cancel_reservation(&mut self, miner: AccountId) -> Result<(), Error> {
            self.ensure_owner()?;
            self.clear_request(miner, REQ_RESERVE);
            self.clear_confirmed_reservation(miner);
            self.env().emit_event(ReservationCancelled { miner });
            Ok(())
        }

        #[ink(message)]
        pub fn vote_extend_reservation(
            &mut self,
            request_hash: Hash,
            miner: AccountId,
            source_tx_hash: String,
        ) -> Result<(), Error> {
            self.ensure_validator()?;
            let caller = self.env().caller();
            let current_block = self.env().block_number();

            // Verify hash
            let computed = Self::compute_extend_hash(&miner, &source_tx_hash);
            if computed != request_hash {
                return Err(Error::HashMismatch);
            }

            // Miner must be active and not already in a swap
            if !self.miner_active.get(miner).unwrap_or(false) {
                return Err(Error::MinerNotActive);
            }
            if self.miner_has_active_swap.get(miner).unwrap_or(false) {
                return Err(Error::MinerHasActiveSwap);
            }

            // Reservation data must exist (a prior reserve quorum succeeded)
            if self.reservation_tao_amount.get(miner).unwrap_or(0) == 0 {
                return Err(Error::NoReservation);
            }

            // Get or create request ID for this extend round
            let request_id = match self.get_active_request(miner, REQ_EXTEND) {
                Some(id) => {
                    if self.request_hash.get(id).unwrap_or_default() != request_hash {
                        return Err(Error::PendingConflict);
                    }
                    id
                }
                None => self.new_request(miner, REQ_EXTEND, request_hash),
            };

            // Record vote
            let vote_count = self.record_vote(request_id, caller)?;

            // Check quorum — extend reservation
            if vote_count >= self.get_required_votes() {
                let new_reserved_until = current_block.saturating_add(self.reservation_ttl);
                self.miner_reserved_until.insert(miner, &new_reserved_until);
                self.clear_request(miner, REQ_EXTEND);

                self.env().emit_event(ReservationExtended {
                    miner,
                    reserved_until: new_reserved_until,
                });
            }

            Ok(())
        }

        // =====================================================================
        // Swap Lifecycle
        // =====================================================================

        #[ink(message)]
        pub fn vote_initiate(
            &mut self,
            request_hash: Hash,
            user: AccountId,
            miner: AccountId,
            source_chain: String,
            dest_chain: String,
            source_amount: Balance,
            tao_amount: Balance,
            user_source_address: String,
            user_dest_address: String,
            source_tx_hash: String,
            source_tx_block: u32,
            dest_amount: Balance,
            miner_source_address: String,
            miner_dest_address: String,
            rate: String,
        ) -> Result<(), Error> {
            self.ensure_validator()?;
            let caller = self.env().caller();
            let current_block = self.env().block_number();

            // Verify hash — covers the full swap shape so no field can be substituted
            // by a malicious validator casting the quorum-reaching vote.
            let computed = Self::compute_initiate_hash(
                &miner,
                &source_tx_hash,
                &source_chain,
                &dest_chain,
                &miner_source_address,
                &miner_dest_address,
                &rate,
                tao_amount,
                source_amount,
                dest_amount,
            );
            if computed != request_hash {
                return Err(Error::HashMismatch);
            }

            // Input validation
            if source_chain == dest_chain {
                return Err(Error::SameChain);
            }
            if source_amount == 0 || tao_amount == 0 {
                return Err(Error::InvalidAmount);
            }
            if source_tx_hash.is_empty() || miner_source_address.is_empty() || miner_dest_address.is_empty() || rate.is_empty() {
                return Err(Error::InputEmpty);
            }
            if source_tx_hash.len() > 128 {
                return Err(Error::InputTooLong);
            }
            if self.used_source_tx.get(&source_tx_hash).unwrap_or(false) {
                return Err(Error::DuplicateSourceTx);
            }

            // Reservation must exist and match.
            // Note: direction is bound via the reserve hash + initiate hash, not
            // via stored state — both hashes cover source_chain/dest_chain, so
            // validator consensus agrees on the direction at both steps.
            let reserved_until = self.miner_reserved_until.get(miner).unwrap_or(0);
            if reserved_until < current_block {
                return Err(Error::NoReservation);
            }
            let res_tao = self.reservation_tao_amount.get(miner).unwrap_or(0);
            let res_source = self.reservation_source_amount.get(miner).unwrap_or(0);
            let res_dest = self.reservation_dest_amount.get(miner).unwrap_or(0);
            if tao_amount != res_tao || source_amount != res_source || dest_amount != res_dest {
                return Err(Error::InvalidAmount);
            }

            // Get or create request ID for this initiate round
            let request_id = match self.get_active_request(miner, REQ_INITIATE) {
                Some(id) => {
                    if self.request_hash.get(id).unwrap_or_default() != request_hash {
                        return Err(Error::PendingConflict);
                    }
                    id
                }
                None => self.new_request(miner, REQ_INITIATE, request_hash),
            };

            // Record vote
            let vote_count = self.record_vote(request_id, caller)?;

            // Check quorum — create swap
            if vote_count >= self.get_required_votes() {
                let miner_collateral = self.collateral.get(miner).unwrap_or(0);
                if tao_amount > miner_collateral {
                    return Err(Error::InsufficientCollateral);
                }

                let swap_id = self.next_swap_id;
                self.next_swap_id = self.next_swap_id.saturating_add(1);

                let swap = SwapData {
                    id: swap_id,
                    user,
                    miner,
                    source_chain,
                    dest_chain,
                    source_amount,
                    dest_amount,
                    tao_amount,
                    user_source_address,
                    user_dest_address,
                    miner_source_address,
                    miner_dest_address,
                    rate,
                    source_tx_hash: source_tx_hash.clone(),
                    source_tx_block,
                    dest_tx_hash: String::new(),
                    dest_tx_block: 0,
                    status: SwapStatus::Active,
                    initiated_block: current_block,
                    timeout_block: current_block.saturating_add(self.fulfillment_timeout_blocks),
                    fulfilled_block: 0,
                    completed_block: 0,
                };

                self.used_source_tx.insert(source_tx_hash, &true);
                self.miner_has_active_swap.insert(miner, &true);
                self.swaps.insert(swap_id, &swap);

                self.clear_confirmed_reservation(miner);
                self.clear_request(miner, REQ_INITIATE);

                self.env().emit_event(SwapInitiated {
                    swap_id,
                    user,
                    miner,
                    source_amount,
                    initiated_block: current_block,
                });
            }

            Ok(())
        }

        /// Mark a swap as fulfilled — miner direct (caller == swap.miner)
        #[ink(message)]
        pub fn mark_fulfilled(
            &mut self,
            swap_id: u64,
            dest_tx_hash: String,
            dest_tx_block: u32,
            dest_amount: Balance,
        ) -> Result<(), Error> {
            let caller = self.env().caller();
            let mut swap = self.swaps.get(swap_id).ok_or(Error::SwapNotFound)?;

            if swap.miner != caller {
                return Err(Error::NotAssignedMiner);
            }
            if swap.status != SwapStatus::Active {
                return Err(Error::InvalidStatus);
            }
            if dest_amount == 0 {
                return Err(Error::InvalidAmount);
            }

            swap.dest_amount = dest_amount;
            swap.status = SwapStatus::Fulfilled;
            swap.dest_tx_hash = dest_tx_hash.clone();
            swap.dest_tx_block = dest_tx_block;
            swap.fulfilled_block = self.env().block_number();
            self.swaps.insert(swap_id, &swap);

            self.env().emit_event(SwapFulfilled {
                swap_id,
                miner: caller,
                dest_tx_hash,
            });
            Ok(())
        }

        /// Confirm a swap — validator-only, quorum mechanism
        #[ink(message)]
        pub fn confirm_swap(&mut self, swap_id: u64) -> Result<(), Error> {
            self.ensure_validator()?;
            let caller = self.env().caller();
            let mut swap = self.swaps.get(swap_id).ok_or(Error::SwapNotFound)?;

            if swap.status != SwapStatus::Fulfilled {
                return Err(Error::InvalidStatus);
            }
            if self.swap_confirm_votes.get((swap_id, caller)).unwrap_or(false) {
                return Err(Error::AlreadyVoted);
            }

            self.swap_confirm_votes.insert((swap_id, caller), &true);
            let vote_count = self.swap_confirm_vote_count.get(swap_id).unwrap_or(0).saturating_add(1);
            self.swap_confirm_vote_count.insert(swap_id, &vote_count);

            self.env().emit_event(VoteCast {
                swap_id,
                validator: caller,
                vote_type: VoteType::Confirm,
                vote_count,
            });

            if vote_count >= self.get_required_votes() {
                swap.status = SwapStatus::Completed;
                swap.completed_block = self.env().block_number();

                // Fee from miner collateral -> accumulated_fees (divisor >= 20 enforced, max 5%)
                #[allow(clippy::arithmetic_side_effects)]
                let fee = swap.tao_amount.saturating_div(self.fee_divisor);
                let miner_collateral = self.collateral.get(swap.miner).unwrap_or(0);
                let actual_fee = core::cmp::min(fee, miner_collateral);
                if actual_fee > 0 {
                    self.collateral.insert(swap.miner, &miner_collateral.saturating_sub(actual_fee));
                    self.accumulated_fees = self.accumulated_fees.saturating_add(actual_fee);
                }

                self.miner_has_active_swap.insert(swap.miner, &false);
                self.miner_last_resolved_block.insert(swap.miner, &swap.completed_block);

                let source_addr = swap.user_source_address.as_bytes().to_vec();
                self.address_strike_count.remove(&source_addr);
                self.address_last_expired.remove(&source_addr);

                self.env().emit_event(SwapCompleted {
                    swap_id,
                    miner: swap.miner,
                    tao_amount: swap.tao_amount,
                    fee_amount: actual_fee,
                });

                self.swaps.remove(swap_id);
                self.swap_confirm_vote_count.remove(swap_id);
                self.swap_timeout_vote_count.remove(swap_id);
                self.clear_request(swap.miner, REQ_EXTEND_TIMEOUT);
            } else {
                self.swaps.insert(swap_id, &swap);
            }
            Ok(())
        }

        /// Timeout a swap — validator-only, quorum mechanism
        #[ink(message)]
        pub fn timeout_swap(&mut self, swap_id: u64) -> Result<(), Error> {
            self.ensure_validator()?;
            let caller = self.env().caller();
            let mut swap = self.swaps.get(swap_id).ok_or(Error::SwapNotFound)?;

            if swap.status != SwapStatus::Active && swap.status != SwapStatus::Fulfilled {
                return Err(Error::InvalidStatus);
            }
            if self.env().block_number() < swap.timeout_block {
                return Err(Error::NotTimedOut);
            }
            if self.swap_timeout_votes.get((swap_id, caller)).unwrap_or(false) {
                return Err(Error::AlreadyVoted);
            }

            self.swap_timeout_votes.insert((swap_id, caller), &true);
            let vote_count = self.swap_timeout_vote_count.get(swap_id).unwrap_or(0).saturating_add(1);
            self.swap_timeout_vote_count.insert(swap_id, &vote_count);

            self.env().emit_event(VoteCast {
                swap_id,
                validator: caller,
                vote_type: VoteType::Timeout,
                vote_count,
            });

            if vote_count >= self.get_required_votes() {
                swap.status = SwapStatus::TimedOut;
                swap.completed_block = self.env().block_number();

                let slash_amount = swap.tao_amount;
                let miner_collateral = self.collateral.get(swap.miner).unwrap_or(0);
                let actual_slash = core::cmp::min(slash_amount, miner_collateral);

                let new_collateral = miner_collateral.saturating_sub(actual_slash);
                if actual_slash > 0 {
                    self.collateral.insert(swap.miner, &new_collateral);

                    if self.env().transfer(swap.user, actual_slash).is_ok() {
                        self.env().emit_event(CollateralSlashed {
                            miner: swap.miner,
                            amount: actual_slash,
                            recipient: swap.user,
                        });
                    } else {
                        self.pending_slashes.insert(swap_id, &(swap.user, actual_slash));
                        self.env().emit_event(SlashPending {
                            swap_id,
                            user: swap.user,
                            amount: actual_slash,
                        });
                    }
                }

                if new_collateral < self.min_collateral {
                    self.miner_active.insert(swap.miner, &false);
                    self.miner_deactivation_block.insert(swap.miner, &self.env().block_number());
                    self.env().emit_event(MinerActivated { miner: swap.miner, active: false });
                }

                self.miner_has_active_swap.insert(swap.miner, &false);
                self.miner_last_resolved_block.insert(swap.miner, &swap.completed_block);

                self.env().emit_event(SwapTimedOut {
                    swap_id,
                    miner: swap.miner,
                    tao_amount: swap.tao_amount,
                    slash_amount: actual_slash,
                });

                self.swaps.remove(swap_id);
                self.swap_confirm_vote_count.remove(swap_id);
                self.swap_timeout_vote_count.remove(swap_id);
                self.clear_request(swap.miner, REQ_EXTEND_TIMEOUT);
            } else {
                self.swaps.insert(swap_id, &swap);
            }
            Ok(())
        }

        /// Extend swap timeout — validator-only, quorum mechanism.
        /// Used when a miner has fulfilled (sent dest funds) but the dest tx
        /// hasn't reached enough confirmations before the timeout expires.
        #[ink(message)]
        pub fn vote_extend_timeout(&mut self, swap_id: u64) -> Result<(), Error> {
            self.ensure_validator()?;
            let caller = self.env().caller();
            let mut swap = self.swaps.get(swap_id).ok_or(Error::SwapNotFound)?;

            if swap.status != SwapStatus::Fulfilled {
                return Err(Error::InvalidStatus);
            }

            // Deterministic hash from swap_id for the request system
            let mut hash_bytes = [0u8; 32];
            hash_bytes[..8].copy_from_slice(&swap_id.to_le_bytes());
            let request_hash = Hash::from(hash_bytes);

            // Get or create request for this extend round
            let request_id = match self.get_active_request(swap.miner, REQ_EXTEND_TIMEOUT) {
                Some(id) => {
                    if self.request_hash.get(id).unwrap_or_default() != request_hash {
                        return Err(Error::PendingConflict);
                    }
                    id
                }
                None => self.new_request(swap.miner, REQ_EXTEND_TIMEOUT, request_hash),
            };

            let vote_count = self.record_vote(request_id, caller)?;

            self.env().emit_event(VoteCast {
                swap_id,
                validator: caller,
                vote_type: VoteType::ExtendTimeout,
                vote_count,
            });

            if vote_count >= self.get_required_votes() {
                let current_block = self.env().block_number();
                let new_timeout = current_block.saturating_add(self.fulfillment_timeout_blocks);
                swap.timeout_block = new_timeout;
                self.swaps.insert(swap_id, &swap);

                // Clear request — allows validators to vote again for another extension
                self.clear_request(swap.miner, REQ_EXTEND_TIMEOUT);

                self.env().emit_event(SwapTimeoutExtended {
                    swap_id,
                    new_timeout_block: new_timeout,
                });
            }

            Ok(())
        }

        /// Claim a pending slash payout (user calls after failed transfer)
        #[ink(message)]
        pub fn claim_slash(&mut self, swap_id: u64) -> Result<(), Error> {
            let caller = self.env().caller();
            let (user, amount) = self.pending_slashes.get(swap_id).ok_or(Error::NoPendingSlash)?;

            if user != caller {
                return Err(Error::InvalidStatus);
            }

            self.pending_slashes.remove(swap_id);
            self.env().transfer(caller, amount).map_err(|_| {
                self.pending_slashes.insert(swap_id, &(user, amount));
                Error::TransferFailed
            })?;

            self.env().emit_event(SlashClaimed {
                swap_id,
                user: caller,
                amount,
            });
            Ok(())
        }

        // =====================================================================
        // Miner Activation / Deactivation
        // =====================================================================

        /// Deactivate a miner — only the miner themselves can deactivate.
        #[ink(message)]
        pub fn deactivate(&mut self, miner: AccountId) -> Result<(), Error> {
            let caller = self.env().caller();
            if caller != miner {
                return Err(Error::NotAssignedMiner);
            }
            self.miner_deactivation_block.insert(miner, &self.env().block_number());
            self.miner_active.insert(miner, &false);
            self.env().emit_event(MinerActivated { miner, active: false });
            Ok(())
        }

        /// Vote to activate a miner — validator-only, quorum required.
        #[ink(message)]
        pub fn vote_activate(&mut self, miner: AccountId) -> Result<(), Error> {
            self.ensure_validator()?;
            self.ensure_not_halted()?;
            let caller = self.env().caller();

            if self.miner_active.get(miner).unwrap_or(false) {
                return Err(Error::InvalidStatus);
            }
            let miner_collateral = self.collateral.get(miner).unwrap_or(0);
            if miner_collateral < self.min_collateral {
                return Err(Error::InsufficientCollateral);
            }

            // Get or create request ID for this activation round
            let request_id = match self.get_active_request(miner, REQ_ACTIVATE) {
                Some(id) => id,
                None => {
                    // No hash needed for activation (just miner identity)
                    let hash = Hash::default();
                    self.new_request(miner, REQ_ACTIVATE, hash)
                }
            };

            // Record vote
            let vote_count = self.record_vote(request_id, caller)?;

            if vote_count >= self.get_required_votes() {
                self.miner_active.insert(miner, &true);
                self.miner_deactivation_block.remove(miner);
                self.clear_request(miner, REQ_ACTIVATE);
                self.env().emit_event(MinerActivated { miner, active: true });
            }

            Ok(())
        }

        // =====================================================================
        // Owner Configuration
        // =====================================================================

        #[ink(message)]
        pub fn transfer_ownership(&mut self, new_owner: AccountId) -> Result<(), Error> {
            self.ensure_owner()?;
            let previous_owner = self.owner;
            self.owner = new_owner;
            self.env().emit_event(OwnershipTransferred { previous_owner, new_owner });
            Ok(())
        }

        #[ink(message)]
        pub fn add_validator(&mut self, validator: AccountId) -> Result<(), Error> {
            self.ensure_owner()?;
            if !self.validators.get(validator).unwrap_or(false) {
                self.validator_count = self.validator_count.saturating_add(1);
            }
            self.validators.insert(validator, &true);
            self.env().emit_event(ValidatorUpdated { validator, registered: true });
            Ok(())
        }

        #[ink(message)]
        pub fn remove_validator(&mut self, validator: AccountId) -> Result<(), Error> {
            self.ensure_owner()?;
            if self.validators.get(validator).unwrap_or(false) {
                self.validator_count = self.validator_count.saturating_sub(1);
            }
            self.validators.remove(validator);
            self.env().emit_event(ValidatorUpdated { validator, registered: false });
            Ok(())
        }

        #[ink(message)]
        pub fn set_fulfillment_timeout(&mut self, blocks: u32) -> Result<(), Error> {
            self.ensure_owner()?;
            if blocks < 10 {
                return Err(Error::InvalidAmount);
            }
            self.fulfillment_timeout_blocks = blocks;
            self.env().emit_event(ConfigUpdated {
                key: String::from("fulfillment_timeout_blocks"),
                value: blocks as u128,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_min_collateral(&mut self, amount: Balance) -> Result<(), Error> {
            self.ensure_owner()?;
            self.min_collateral = amount;
            self.env().emit_event(ConfigUpdated {
                key: String::from("min_collateral"),
                value: amount,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_max_collateral(&mut self, amount: Balance) -> Result<(), Error> {
            self.ensure_owner()?;
            self.max_collateral = amount;
            self.env().emit_event(ConfigUpdated {
                key: String::from("max_collateral"),
                value: amount,
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
        pub fn set_min_swap_amount(&mut self, amount: Balance) -> Result<(), Error> {
            self.ensure_owner()?;
            if amount > 0 && amount < 100 {
                return Err(Error::InvalidAmount);
            }
            self.min_swap_amount = amount;
            self.env().emit_event(ConfigUpdated {
                key: String::from("min_swap_amount"),
                value: amount,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_max_swap_amount(&mut self, amount: Balance) -> Result<(), Error> {
            self.ensure_owner()?;
            self.max_swap_amount = amount;
            self.env().emit_event(ConfigUpdated {
                key: String::from("max_swap_amount"),
                value: amount,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_recycle_address(&mut self, address: AccountId) -> Result<(), Error> {
            self.ensure_owner()?;
            self.recycle_address = address;
            Ok(())
        }

        #[ink(message)]
        pub fn set_reservation_ttl(&mut self, blocks: u32) -> Result<(), Error> {
            self.ensure_owner()?;
            self.reservation_ttl = blocks;
            self.env().emit_event(ConfigUpdated {
                key: String::from("reservation_ttl"),
                value: blocks as u128,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn set_fee_divisor(&mut self, divisor: u128) -> Result<(), Error> {
            self.ensure_owner()?;
            if divisor < MIN_FEE_DIVISOR {
                return Err(Error::InvalidAmount);
            }
            self.fee_divisor = divisor;
            self.env().emit_event(ConfigUpdated {
                key: String::from("fee_divisor"),
                value: divisor,
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

        #[ink(message)]
        pub fn recycle_fees(&mut self) -> Result<(), Error> {
            self.ensure_owner()?;

            let fees = self.accumulated_fees;
            if fees == 0 {
                return Err(Error::ZeroAmount);
            }

            self.env().transfer(self.recycle_address, fees)
                .map_err(|_| Error::TransferFailed)?;

            self.accumulated_fees = 0;
            self.total_recycled_fees = self.total_recycled_fees.saturating_add(fees);
            self.env().emit_event(FeesRecycled { tao_amount: fees });
            Ok(())
        }

        // =====================================================================
        // Query Functions
        // =====================================================================

        #[ink(message)]
        pub fn get_swap(&self, swap_id: u64) -> Option<SwapData> {
            self.swaps.get(swap_id)
        }

        #[ink(message)]
        pub fn get_collateral(&self, hotkey: AccountId) -> Balance {
            self.collateral.get(hotkey).unwrap_or(0)
        }

        #[ink(message)]
        pub fn get_miner_active(&self, hotkey: AccountId) -> bool {
            self.miner_active.get(hotkey).unwrap_or(false)
        }

        #[ink(message)]
        pub fn get_miner_has_active_swap(&self, hotkey: AccountId) -> bool {
            self.miner_has_active_swap.get(hotkey).unwrap_or(false)
        }

        #[ink(message)]
        pub fn get_miner_last_resolved_block(&self, miner: AccountId) -> u32 {
            self.miner_last_resolved_block.get(miner).unwrap_or(0)
        }

        #[ink(message)]
        pub fn is_validator(&self, account: AccountId) -> bool {
            self.validators.get(account).unwrap_or(false)
        }

        #[ink(message)]
        pub fn get_next_swap_id(&self) -> u64 {
            self.next_swap_id
        }

        #[ink(message)]
        pub fn get_fulfillment_timeout(&self) -> u32 {
            self.fulfillment_timeout_blocks
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
        pub fn get_required_votes_count(&self) -> u32 {
            self.get_required_votes()
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
        pub fn get_owner(&self) -> AccountId {
            self.owner
        }

        #[ink(message)]
        pub fn get_halted(&self) -> bool {
            self.halted
        }

        #[ink(message)]
        pub fn get_recycle_address(&self) -> AccountId {
            self.recycle_address
        }

        #[ink(message)]
        pub fn get_pending_slash(&self, swap_id: u64) -> Balance {
            self.pending_slashes.get(swap_id).map(|(_, amount)| amount).unwrap_or(0)
        }

        #[ink(message)]
        pub fn get_min_swap_amount(&self) -> Balance {
            self.min_swap_amount
        }

        #[ink(message)]
        pub fn get_max_swap_amount(&self) -> Balance {
            self.max_swap_amount
        }

        #[ink(message)]
        pub fn get_miner_reserved_until(&self, miner: AccountId) -> u32 {
            self.miner_reserved_until.get(miner).unwrap_or(0)
        }

        #[ink(message)]
        pub fn get_reservation_ttl(&self) -> u32 {
            self.reservation_ttl
        }

        #[ink(message)]
        pub fn get_fee_divisor(&self) -> u128 {
            self.fee_divisor
        }

        #[ink(message)]
        pub fn get_miner_deactivation_block(&self, miner: AccountId) -> u32 {
            self.miner_deactivation_block.get(miner).unwrap_or(0)
        }

        #[ink(message)]
        pub fn get_consensus_threshold(&self) -> u8 {
            self.consensus_threshold_percent
        }

        #[ink(message)]
        pub fn get_validator_count(&self) -> u32 {
            self.validator_count
        }

        #[ink(message)]
        pub fn get_activation_vote_count(&self, miner: AccountId) -> u32 {
            match self.miner_active_request.get((miner, REQ_ACTIVATE)) {
                Some(id) => self.request_vote_count.get(id).unwrap_or(0),
                None => 0,
            }
        }

        #[ink(message)]
        pub fn get_reservation_data(
            &self,
            miner: AccountId,
        ) -> Option<(Vec<u8>, Balance, Balance, Balance, u32)> {
            let reserved_until = self.miner_reserved_until.get(miner).unwrap_or(0);
            if reserved_until == 0 {
                return None;
            }
            Some((
                self.reservation_source_addr.get(miner).unwrap_or_default(),
                self.reservation_tao_amount.get(miner).unwrap_or(0),
                self.reservation_source_amount.get(miner).unwrap_or(0),
                self.reservation_dest_amount.get(miner).unwrap_or(0),
                reserved_until,
            ))
        }

        #[ink(message)]
        pub fn get_pending_reserve_vote_count(&self, miner: AccountId) -> u32 {
            match self.miner_active_request.get((miner, REQ_RESERVE)) {
                Some(id) => self.request_vote_count.get(id).unwrap_or(0),
                None => 0,
            }
        }

        #[ink(message)]
        pub fn get_extend_vote_count(&self, miner: AccountId) -> u32 {
            match self.miner_active_request.get((miner, REQ_EXTEND)) {
                Some(id) => self.request_vote_count.get(id).unwrap_or(0),
                None => 0,
            }
        }

        #[ink(message)]
        pub fn get_cooldown(&self, source_address: Vec<u8>) -> (u8, u32) {
            (
                self.address_strike_count.get(&source_address).unwrap_or(0),
                self.address_last_expired.get(&source_address).unwrap_or(0),
            )
        }
    }
}

#![cfg_attr(not(feature = "std"), no_std, no_main)]

mod types;
mod errors;
mod events;

use types::{Reservation, SwapData, SwapStatus, VoteType};
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
        halted: bool,
        // Whitelisted validator set. A Vec (not Mapping) because we need to
        // enumerate for `get_validators()` and Ink! mappings aren't iterable.
        // N is bounded to a handful of validators in practice, so the O(N)
        // `contains` in ensure_validator costs nothing. Replaces the earlier
        // Mapping<AccountId,bool> + separate validator_count counter.
        validators: Vec<AccountId>,

        // Swap state
        next_swap_id: u64,
        swaps: Mapping<u64, SwapData>,
        used_from_tx: Mapping<String, bool>,

        // Miner state
        collateral: Mapping<AccountId, Balance>,
        miner_active: Mapping<AccountId, bool>,
        miner_has_active_swap: Mapping<AccountId, bool>,
        miner_deactivation_block: Mapping<AccountId, u32>,

        // Consensus voting — all vote types use unique request IDs (like swap IDs).
        // Voters held as a Vec per request so the entire round drops in one op
        // on quorum/expiry; a per-(request, validator) Mapping leaves trie
        // entries behind forever because ink! Mappings aren't iterable.
        next_request_id: u64,
        request_voters: Mapping<u64, Vec<AccountId>>,
        request_created: Mapping<u64, u32>,
        request_hash: Mapping<u64, Hash>,

        // Active request ID per miner per miner-keyed vote type
        // (activation/reserve/initiate/extend/deactivate/extend-timeout).
        miner_active_request: Mapping<(AccountId, u8), u64>,

        // Active request ID per swap per swap-keyed vote type (confirm/timeout).
        // Uses the same request_* vote tables as miner-keyed votes.
        pending_swap_votes: Mapping<(u64, u8), u64>,

        // Confirmed reservations (post-quorum); absorbs the prior six
        // reservation_* / miner_reserved_until Mappings into one struct.
        reservations: Mapping<AccountId, Reservation>,

        // Cooldown strike tracking (lazy eval) — (strike_count, last_expired_block)
        address_cooldown: Mapping<String, (u8, u32)>,
        // Financials
        accumulated_fees: Balance,
        total_recycled_fees: Balance,
        pending_slashes: Mapping<u64, (AccountId, Balance)>,
    }

    // Request type constants — miner-keyed
    const REQ_ACTIVATE: u8 = 0;
    const REQ_RESERVE: u8 = 1;
    const REQ_INITIATE: u8 = 2;
    const REQ_EXTEND: u8 = 3;
    const REQ_EXTEND_TIMEOUT: u8 = 4;
    const REQ_DEACTIVATE: u8 = 5;
    // Swap-keyed request types (used with pending_swap_votes).
    const REQ_CONFIRM: u8 = 6;
    const REQ_TIMEOUT: u8 = 7;

    // Hardcoded 1% protocol fee. Immutable — not even the owner can change it.
    // Callers on both the miner and validator side hardcode the same value so
    // no one needs to poll the contract to compute fee_amount.
    const FEE_DIVISOR: u128 = 100;

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
            // Validator set is tiny by policy (bounded handful); saturating at
            // u32::MAX is defensive only. try_from lets clippy see the bound.
            let count = u32::try_from(self.validators.len()).unwrap_or(u32::MAX);
            if count == 0 {
                return 1;
            }
            let numerator = count.saturating_mul(self.consensus_threshold_percent as u32);
            let required = numerator.saturating_add(99) / 100;
            core::cmp::max(1, required)
        }

        /// Keccak-hash any SCALE-encodable value. Call sites pass the full
        /// tuple of fields bound into the request hash — the hash algorithm
        /// and field order must match the off-chain signer, so keep these
        /// tuples stable when refactoring.
        fn hash_request<T: scale::Encode>(value: &T) -> Hash {
            let mut output = <ink::env::hash::Keccak256 as ink::env::hash::HashOutput>::Type::default();
            ink::env::hash_encoded::<ink::env::hash::Keccak256, _>(value, &mut output);
            Hash::from(output)
        }

        fn clear_confirmed_reservation(&mut self, miner: AccountId) {
            self.reservations.remove(miner);
        }

        fn reserved_until_of(&self, miner: AccountId) -> u32 {
            self.reservations.get(miner).map(|r| r.reserved_until).unwrap_or(0)
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

        /// Record a vote on a request. Returns the new vote count.
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

        /// Return the active request ID for (miner, req_type), or clear it and
        /// return None if it's expired. Mutates state — the name makes that
        /// contract explicit so callers don't assume a side-effect-free read.
        fn take_or_expire_active_request(&mut self, miner: AccountId, req_type: u8) -> Option<u64> {
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
            self.request_voters.remove(request_id);
            self.request_created.remove(request_id);
            self.request_hash.remove(request_id);
        }

        /// Shared consensus-vote flow for miner-keyed request types (reserve,
        /// initiate, activate, deactivate, extend-reservation, extend-timeout).
        ///
        /// Resolves or allocates a request_id keyed by (miner, req_type), records
        /// the caller's vote, and runs `on_quorum` once the quorum is reached.
        /// Votes on an existing round are rejected when `request_hash` differs
        /// (PendingConflict) — callers that don't bind a hash pass Hash::default().
        fn consensus_vote<F>(
            &mut self,
            miner: AccountId,
            req_type: u8,
            request_hash: Hash,
            on_quorum: F,
        ) -> Result<(), Error>
        where
            F: FnOnce(&mut Self) -> Result<(), Error>,
        {
            let caller = self.env().caller();
            let id = match self.take_or_expire_active_request(miner, req_type) {
                Some(id) => {
                    if self.request_hash.get(id).unwrap_or_default() != request_hash {
                        return Err(Error::PendingConflict);
                    }
                    id
                }
                None => self.new_request(miner, req_type, request_hash),
            };
            let votes = self.record_vote(id, caller)?;
            if votes >= self.get_required_votes() {
                on_quorum(self)?;
                self.clear_request(miner, req_type);
            }
            Ok(())
        }

        /// Shared consensus-vote flow for swap-keyed request types
        /// (confirm/timeout/extend_timeout).
        ///
        /// Allocates a request_id keyed by (swap_id, req_type), emits a
        /// VoteCast event on every vote so dashboards can track progress,
        /// and runs `on_quorum` once the quorum is reached. The hash is
        /// derived from (swap_id, req_type) so every validator binds the
        /// same round without needing to sign anything off-chain.
        fn consensus_swap_vote<F>(
            &mut self,
            swap_id: u64,
            req_type: u8,
            vote_type: VoteType,
            on_quorum: F,
        ) -> Result<(), Error>
        where
            F: FnOnce(&mut Self) -> Result<(), Error>,
        {
            let caller = self.env().caller();
            let mut hash_bytes = [0u8; 32];
            hash_bytes[..8].copy_from_slice(&swap_id.to_le_bytes());
            hash_bytes[8] = req_type;
            let request_hash = Hash::from(hash_bytes);

            let id = match self.pending_swap_votes.get((swap_id, req_type)) {
                Some(id) => id,
                None => {
                    let new_id = self.next_request_id;
                    self.next_request_id = new_id.saturating_add(1);
                    self.request_hash.insert(new_id, &request_hash);
                    self.request_created.insert(new_id, &self.env().block_number());
                    self.pending_swap_votes.insert((swap_id, req_type), &new_id);
                    new_id
                }
            };
            let votes = self.record_vote(id, caller)?;

            self.env().emit_event(VoteCast {
                swap_id,
                validator: caller,
                vote_type,
                vote_count: votes,
            });

            if votes >= self.get_required_votes() {
                on_quorum(self)?;
                self.clear_request_data(id);
                self.pending_swap_votes.remove((swap_id, req_type));
            }
            Ok(())
        }

        /// Clear every pending swap-vote round for a resolved swap.
        fn clear_pending_swap_votes(&mut self, swap_id: u64) {
            for req_type in [REQ_CONFIRM, REQ_TIMEOUT, REQ_EXTEND_TIMEOUT] {
                if let Some(id) = self.pending_swap_votes.get((swap_id, req_type)) {
                    self.clear_request_data(id);
                    self.pending_swap_votes.remove((swap_id, req_type));
                }
            }
        }

        /// Deduct up to `amount` from a miner's collateral, clamped to what
        /// they hold. Auto-deactivates the miner if the remaining balance falls
        /// below min_collateral while they're still flagged active. Returns the
        /// amount actually deducted.
        ///
        /// Shared between confirm_swap (fee) and timeout_swap (slash) so the
        /// floor-breach guard lives in one place and can't drift between them.
        fn apply_collateral_penalty(&mut self, miner: AccountId, amount: Balance) -> Balance {
            let current = self.collateral.get(miner).unwrap_or(0);
            let actual = core::cmp::min(amount, current);
            if actual == 0 {
                return 0;
            }
            let remaining = current.saturating_sub(actual);
            self.collateral.insert(miner, &remaining);
            if remaining < self.min_collateral && self.miner_active.get(miner).unwrap_or(false) {
                self.miner_active.insert(miner, &false);
                self.miner_deactivation_block.insert(miner, &self.env().block_number());
                self.env().emit_event(MinerActivated { miner, active: false });
            }
            actual
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
                halted: false,
                validators: Vec::new(),

                next_swap_id: 1,
                swaps: Mapping::default(),
                used_from_tx: Mapping::default(),

                collateral: Mapping::default(),
                miner_active: Mapping::default(),
                miner_has_active_swap: Mapping::default(),
                miner_deactivation_block: Mapping::default(),

                next_request_id: 1,
                request_voters: Mapping::default(),
                request_created: Mapping::default(),
                request_hash: Mapping::default(),
                miner_active_request: Mapping::default(),
                pending_swap_votes: Mapping::default(),

                reservations: Mapping::default(),

                address_cooldown: Mapping::default(),
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
                amount,
                total: new_total,
            });
            Ok(())
        }

        #[ink(message)]
        pub fn withdraw_collateral(&mut self, amount: Balance) -> Result<(), Error> {
            let caller = self.env().caller();
            if amount == 0 {
                return Err(Error::InvalidAmount);
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

            if self.reserved_until_of(caller) >= self.env().block_number() {
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
            user_from_address: String,
            from_chain: String,
            to_chain: String,
            tao_amount: Balance,
            from_amount: Balance,
            to_amount: Balance,
        ) -> Result<(), Error> {
            self.ensure_validator()?;
            self.ensure_not_halted()?;
            let current_block = self.env().block_number();

            // Verify hash — from_chain and to_chain are included in the hash,
            // so validators must agree on the direction. No separate check needed.
            let computed = Self::hash_request(&(
                &miner,
                &user_from_address,
                &from_chain,
                &to_chain,
                tao_amount,
                from_amount,
                to_amount,
            ));
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
            let existing = self.reservations.get(miner);
            let reserved_until = existing.as_ref().map(|r| r.reserved_until).unwrap_or(0);
            if reserved_until >= current_block {
                return Err(Error::MinerReserved);
            }
            // Lazy strike: expired confirmed reservation -> record strike
            if let Some(prev) = existing {
                let (strikes, _) = self.address_cooldown.get(&prev.from_addr).unwrap_or((0, 0));
                self.address_cooldown.insert(&prev.from_addr, &(strikes.saturating_add(1), current_block));
                self.clear_confirmed_reservation(miner);
                self.env().emit_event(ReservationCancelled { miner });
            }

            self.consensus_vote(miner, REQ_RESERVE, request_hash, move |this| {
                let new_reserved_until = this.env().block_number().saturating_add(this.reservation_ttl);
                this.reservations.insert(
                    miner,
                    &Reservation {
                        hash: request_hash,
                        from_addr: user_from_address,
                        tao_amount,
                        from_amount,
                        to_amount,
                        reserved_until: new_reserved_until,
                    },
                );
                this.env().emit_event(MinerReserved {
                    miner,
                    reserved_until: new_reserved_until,
                });
                Ok(())
            })
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
            from_tx_hash: String,
        ) -> Result<(), Error> {
            self.ensure_validator()?;

            // Verify hash
            let computed = Self::hash_request(&(&miner, &from_tx_hash));
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
            if self.reservations.get(miner).is_none() {
                return Err(Error::NoReservation);
            }

            self.consensus_vote(miner, REQ_EXTEND, request_hash, move |this| {
                // Re-read on quorum — the pre-check above already proved it exists
                // at call time, but the closure runs after consensus so we reload.
                let Some(mut reservation) = this.reservations.get(miner) else {
                    return Err(Error::NoReservation);
                };
                let new_reserved_until =
                    this.env().block_number().saturating_add(this.reservation_ttl);
                reservation.reserved_until = new_reserved_until;
                this.reservations.insert(miner, &reservation);
                this.env().emit_event(ReservationExtended {
                    miner,
                    reserved_until: new_reserved_until,
                });
                Ok(())
            })
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
            from_chain: String,
            to_chain: String,
            from_amount: Balance,
            tao_amount: Balance,
            user_from_address: String,
            user_to_address: String,
            from_tx_hash: String,
            from_tx_block: u32,
            to_amount: Balance,
            miner_from_address: String,
            miner_to_address: String,
            rate: String,
        ) -> Result<(), Error> {
            self.ensure_validator()?;
            let current_block = self.env().block_number();

            // Verify hash — covers the full swap shape so no field can be substituted
            // by a malicious validator casting the quorum-reaching vote.
            let computed = Self::hash_request(&(
                &miner,
                &from_tx_hash,
                &from_chain,
                &to_chain,
                &miner_from_address,
                &miner_to_address,
                &rate,
                tao_amount,
                from_amount,
                to_amount,
            ));
            if computed != request_hash {
                return Err(Error::HashMismatch);
            }

            // Input validation
            if from_chain == to_chain {
                return Err(Error::SameChain);
            }
            if from_amount == 0 || tao_amount == 0 {
                return Err(Error::InvalidAmount);
            }
            if from_tx_hash.is_empty() || miner_from_address.is_empty() || miner_to_address.is_empty() || rate.is_empty() {
                return Err(Error::InputEmpty);
            }
            if from_tx_hash.len() > 128 {
                return Err(Error::InputTooLong);
            }
            if self.used_from_tx.get(&from_tx_hash).unwrap_or(false) {
                return Err(Error::DuplicateSourceTx);
            }

            // Reservation must exist and match.
            // Note: direction is bound via the reserve hash + initiate hash, not
            // via stored state — both hashes cover from_chain/to_chain, so
            // validator consensus agrees on the direction at both steps.
            let Some(reservation) = self.reservations.get(miner) else {
                return Err(Error::NoReservation);
            };
            if reservation.reserved_until < current_block {
                return Err(Error::NoReservation);
            }
            if tao_amount != reservation.tao_amount
                || from_amount != reservation.from_amount
                || to_amount != reservation.to_amount
            {
                return Err(Error::InvalidAmount);
            }
            // Caller must be the address that actually reserved the miner.
            // Without this check, a second user who sends the quoted amount to
            // the miner can hijack an active reservation, consuming the slot
            // intended for the original reserver.
            if user_from_address != reservation.from_addr {
                return Err(Error::NoReservation);
            }

            self.consensus_vote(miner, REQ_INITIATE, request_hash, move |this| {
                let miner_collateral = this.collateral.get(miner).unwrap_or(0);
                if tao_amount > miner_collateral {
                    return Err(Error::InsufficientCollateral);
                }

                let swap_id = this.next_swap_id;
                this.next_swap_id = this.next_swap_id.saturating_add(1);
                let current_block = this.env().block_number();

                let swap = SwapData {
                    id: swap_id,
                    user,
                    miner,
                    from_chain,
                    to_chain,
                    from_amount,
                    to_amount,
                    tao_amount,
                    user_from_address,
                    user_to_address,
                    miner_from_address,
                    miner_to_address,
                    rate,
                    from_tx_hash: from_tx_hash.clone(),
                    from_tx_block,
                    to_tx_hash: String::new(),
                    to_tx_block: 0,
                    status: SwapStatus::Active,
                    initiated_block: current_block,
                    timeout_block: current_block.saturating_add(this.fulfillment_timeout_blocks),
                    fulfilled_block: 0,
                    completed_block: 0,
                };

                this.used_from_tx.insert(from_tx_hash, &true);
                this.miner_has_active_swap.insert(miner, &true);
                this.swaps.insert(swap_id, &swap);

                this.clear_confirmed_reservation(miner);

                this.env().emit_event(SwapInitiated {
                    swap_id,
                    user,
                    miner,
                    from_amount,
                    initiated_block: current_block,
                });
                Ok(())
            })
        }

        /// Mark a swap as fulfilled — miner direct (caller == swap.miner)
        #[ink(message)]
        pub fn mark_fulfilled(
            &mut self,
            swap_id: u64,
            to_tx_hash: String,
            to_tx_block: u32,
            to_amount: Balance,
        ) -> Result<(), Error> {
            let caller = self.env().caller();
            let mut swap = self.swaps.get(swap_id).ok_or(Error::SwapNotFound)?;

            if swap.miner != caller {
                return Err(Error::NotAssignedMiner);
            }
            if swap.status != SwapStatus::Active {
                return Err(Error::InvalidStatus);
            }
            if to_amount == 0 {
                return Err(Error::InvalidAmount);
            }

            swap.to_amount = to_amount;
            swap.status = SwapStatus::Fulfilled;
            swap.to_tx_hash = to_tx_hash.clone();
            swap.to_tx_block = to_tx_block;
            swap.fulfilled_block = self.env().block_number();
            self.swaps.insert(swap_id, &swap);

            self.env().emit_event(SwapFulfilled {
                swap_id,
                miner: caller,
                to_tx_hash,
            });
            Ok(())
        }

        /// Confirm a swap — validator-only, quorum mechanism
        #[ink(message)]
        pub fn confirm_swap(&mut self, swap_id: u64) -> Result<(), Error> {
            self.ensure_validator()?;
            let swap = self.swaps.get(swap_id).ok_or(Error::SwapNotFound)?;

            if swap.status != SwapStatus::Fulfilled {
                return Err(Error::InvalidStatus);
            }

            self.consensus_swap_vote(swap_id, REQ_CONFIRM, VoteType::Confirm, move |this| {
                let mut swap = match this.swaps.get(swap_id) {
                    Some(s) => s,
                    None => return Err(Error::SwapNotFound),
                };
                swap.status = SwapStatus::Completed;
                swap.completed_block = this.env().block_number();

                // Fee from miner collateral -> accumulated_fees. 1% hardcoded.
                // apply_collateral_penalty auto-deactivates the miner if this
                // drops them below min_collateral, keeping the guard in sync
                // with timeout_swap.
                #[allow(clippy::arithmetic_side_effects)]
                let fee = swap.tao_amount.saturating_div(FEE_DIVISOR);
                let actual_fee = this.apply_collateral_penalty(swap.miner, fee);
                if actual_fee > 0 {
                    this.accumulated_fees = this.accumulated_fees.saturating_add(actual_fee);
                }

                this.miner_has_active_swap.insert(swap.miner, &false);
                this.address_cooldown.remove(&swap.user_from_address);

                this.env().emit_event(SwapCompleted {
                    swap_id,
                    miner: swap.miner,
                    tao_amount: swap.tao_amount,
                    fee_amount: actual_fee,
                });

                this.swaps.remove(swap_id);
                this.clear_pending_swap_votes(swap_id);
                Ok(())
            })
        }

        /// Timeout a swap — validator-only, quorum mechanism
        #[ink(message)]
        pub fn timeout_swap(&mut self, swap_id: u64) -> Result<(), Error> {
            self.ensure_validator()?;
            let swap = self.swaps.get(swap_id).ok_or(Error::SwapNotFound)?;

            if swap.status != SwapStatus::Active && swap.status != SwapStatus::Fulfilled {
                return Err(Error::InvalidStatus);
            }
            if self.env().block_number() < swap.timeout_block {
                return Err(Error::NotTimedOut);
            }

            self.consensus_swap_vote(swap_id, REQ_TIMEOUT, VoteType::Timeout, move |this| {
                let mut swap = match this.swaps.get(swap_id) {
                    Some(s) => s,
                    None => return Err(Error::SwapNotFound),
                };
                swap.status = SwapStatus::TimedOut;
                swap.completed_block = this.env().block_number();

                // Slash miner collateral up to the full tao_amount; the helper
                // also auto-deactivates the miner if this drops them below
                // min_collateral.
                let actual_slash = this.apply_collateral_penalty(swap.miner, swap.tao_amount);
                if actual_slash > 0 {
                    if this.env().transfer(swap.user, actual_slash).is_ok() {
                        this.env().emit_event(CollateralSlashed {
                            miner: swap.miner,
                            amount: actual_slash,
                            recipient: swap.user,
                        });
                    } else {
                        this.pending_slashes.insert(swap_id, &(swap.user, actual_slash));
                        this.env().emit_event(SlashPending {
                            swap_id,
                            user: swap.user,
                            amount: actual_slash,
                        });
                    }
                }

                this.miner_has_active_swap.insert(swap.miner, &false);

                this.env().emit_event(SwapTimedOut {
                    swap_id,
                    miner: swap.miner,
                    tao_amount: swap.tao_amount,
                    slash_amount: actual_slash,
                });

                this.swaps.remove(swap_id);
                this.clear_pending_swap_votes(swap_id);
                Ok(())
            })
        }

        /// Extend swap timeout — validator-only, quorum mechanism.
        /// Used when a miner has fulfilled (sent dest funds) but the dest tx
        /// hasn't reached enough confirmations before the timeout expires.
        #[ink(message)]
        pub fn vote_extend_timeout(&mut self, swap_id: u64) -> Result<(), Error> {
            self.ensure_validator()?;
            let swap = self.swaps.get(swap_id).ok_or(Error::SwapNotFound)?;

            if swap.status != SwapStatus::Fulfilled {
                return Err(Error::InvalidStatus);
            }

            self.consensus_swap_vote(swap_id, REQ_EXTEND_TIMEOUT, VoteType::ExtendTimeout, move |this| {
                let mut swap = match this.swaps.get(swap_id) {
                    Some(s) => s,
                    None => return Err(Error::SwapNotFound),
                };
                let new_timeout = this.env().block_number().saturating_add(this.fulfillment_timeout_blocks);
                swap.timeout_block = new_timeout;
                this.swaps.insert(swap_id, &swap);
                // clear_pending_swap_votes runs in consensus_swap_vote after this
                // closure via the standard cleanup path — the caller can vote
                // again for another extension on the next round.
                this.env().emit_event(SwapTimeoutExtended {
                    swap_id,
                    new_timeout_block: new_timeout,
                });
                Ok(())
            })
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

        /// Deactivate a miner — only the miner themselves can deactivate, and only
        /// when idle. Blocks mid-swap or while reserved so miners cannot dodge
        /// in-flight obligations via self-deactivation.
        #[ink(message)]
        pub fn deactivate(&mut self, miner: AccountId) -> Result<(), Error> {
            let caller = self.env().caller();
            if caller != miner {
                return Err(Error::NotAssignedMiner);
            }
            if self.miner_has_active_swap.get(miner).unwrap_or(false) {
                return Err(Error::MinerHasActiveSwap);
            }
            if self.reserved_until_of(miner) >= self.env().block_number() {
                return Err(Error::MinerReserved);
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

            if self.miner_active.get(miner).unwrap_or(false) {
                return Err(Error::InvalidStatus);
            }
            let miner_collateral = self.collateral.get(miner).unwrap_or(0);
            if miner_collateral < self.min_collateral {
                return Err(Error::InsufficientCollateral);
            }

            // Activation hash is just the miner identity — deterministic so every
            // validator binds the same round.
            self.consensus_vote(miner, REQ_ACTIVATE, Hash::default(), move |this| {
                this.miner_active.insert(miner, &true);
                this.miner_deactivation_block.remove(miner);
                this.env().emit_event(MinerActivated { miner, active: true });
                Ok(())
            })
        }

        /// Vote to deactivate a miner — validator-only, quorum required.
        ///
        /// Trust-based: on quorum the miner's active flag is cleared, full stop.
        /// No collateral, status, or balance gates beyond "currently active".
        /// Deliberately unconstrained so the validator consensus can cover any
        /// remediation case (min_collateral raise, protocol abuse, operational
        /// emergencies). Abuse protection comes from the quorum itself — the
        /// same trust envelope as `vote_activate` / `vote_reserve`.
        ///
        /// Not blocked mid-swap: the existing swap lifecycle proceeds via its
        /// persisted assignment. Miner cannot re-activate while below the
        /// collateral floor because `vote_activate` still checks it.
        #[ink(message)]
        pub fn vote_deactivate(&mut self, miner: AccountId) -> Result<(), Error> {
            self.ensure_validator()?;

            if !self.miner_active.get(miner).unwrap_or(false) {
                return Err(Error::InvalidStatus);
            }

            self.consensus_vote(miner, REQ_DEACTIVATE, Hash::default(), move |this| {
                this.miner_active.insert(miner, &false);
                this.miner_deactivation_block.insert(miner, &this.env().block_number());
                this.env().emit_event(MinerActivated { miner, active: false });
                Ok(())
            })
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
            // Idempotent: double-add is a no-op rather than an error, matching
            // the prior Mapping-based behaviour.
            if !self.validators.contains(&validator) {
                self.validators.push(validator);
            }
            self.env().emit_event(ValidatorUpdated { validator, registered: true });
            Ok(())
        }

        #[ink(message)]
        pub fn remove_validator(&mut self, validator: AccountId) -> Result<(), Error> {
            self.ensure_owner()?;
            // Idempotent: removing a non-member is a no-op.
            self.validators.retain(|v| v != &validator);
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
                return Err(Error::InvalidAmount);
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
        pub fn is_validator(&self, account: AccountId) -> bool {
            self.validators.contains(&account)
        }

        /// Returns the full whitelisted validator set. O(N) storage read with N
        /// bounded by the (small) validator count. Callers wanting just the
        /// count should prefer `get_validator_count` which skips the clone.
        #[ink(message)]
        pub fn get_validators(&self) -> Vec<AccountId> {
            self.validators.clone()
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
            self.reserved_until_of(miner)
        }

        #[ink(message)]
        pub fn get_reservation_ttl(&self) -> u32 {
            self.reservation_ttl
        }

        #[ink(message)]
        pub fn get_miner_deactivation_block(&self, miner: AccountId) -> u32 {
            self.miner_deactivation_block.get(miner).unwrap_or(0)
        }

        /// Composite miner read: (collateral, active, has_active_swap,
        /// reserved_until, deactivation_block). One RPC for the CLI views
        /// that otherwise make four to six separate contract reads per
        /// render.
        #[ink(message)]
        pub fn get_miner_snapshot(
            &self,
            miner: AccountId,
        ) -> (Balance, bool, bool, u32, u32) {
            (
                self.collateral.get(miner).unwrap_or(0),
                self.miner_active.get(miner).unwrap_or(false),
                self.miner_has_active_swap.get(miner).unwrap_or(false),
                self.reserved_until_of(miner),
                self.miner_deactivation_block.get(miner).unwrap_or(0),
            )
        }

        #[ink(message)]
        pub fn get_consensus_threshold(&self) -> u8 {
            self.consensus_threshold_percent
        }

        #[ink(message)]
        pub fn get_validator_count(&self) -> u32 {
            u32::try_from(self.validators.len()).unwrap_or(u32::MAX)
        }

        /// Returns the reservation amounts (tao, from, to) if one exists.
        /// Callers that also need `reserved_until` can query
        /// `get_miner_reserved_until` separately — it's a cheap single-field
        /// read on the same struct.
        #[ink(message)]
        pub fn get_reservation_data(
            &self,
            miner: AccountId,
        ) -> Option<(Balance, Balance, Balance)> {
            self.reservations
                .get(miner)
                .map(|r| (r.tao_amount, r.from_amount, r.to_amount))
        }

        #[ink(message)]
        pub fn get_pending_reserve_vote_count(&self, miner: AccountId) -> u32 {
            match self.miner_active_request.get((miner, REQ_RESERVE)) {
                Some(id) => self
                    .request_voters
                    .get(id)
                    .map(|v| u32::try_from(v.len()).unwrap_or(u32::MAX))
                    .unwrap_or(0),
                None => 0,
            }
        }

        #[ink(message)]
        pub fn get_cooldown(&self, from_address: String) -> (u8, u32) {
            self.address_cooldown.get(&from_address).unwrap_or((0, 0))
        }
    }
}

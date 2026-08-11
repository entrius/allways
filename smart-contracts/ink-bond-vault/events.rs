use ink::primitives::{AccountId, Hash};

/// Miner posted TAO into the vault
#[ink::event]
pub struct CollateralPosted {
    #[ink(topic)]
    pub miner: AccountId,
    pub amount: u128,
    pub total: u128,
}

/// Miner withdrew TAO from the vault (only possible while unlocked)
#[ink::event]
pub struct CollateralWithdrawn {
    #[ink(topic)]
    pub miner: AccountId,
    pub amount: u128,
    pub remaining: u128,
}

/// Lock transition. `locked = true` is miner-initiated (`lock_bond`);
/// `locked = false` comes from a `vote_unlock` quorum.
#[ink::event]
pub struct BondLockChanged {
    #[ink(topic)]
    pub miner: AccountId,
    pub locked: bool,
    pub epoch: u64,
}

/// A validator voted on an unlock, slash, or fee-settle round.
/// `subject` identifies the round's target through one 32-byte field:
/// the miner (unlock), the swap_ref (slash), or the batch hash (collect).
#[ink::event]
pub struct VaultVoteCast {
    #[ink(topic)]
    pub validator: AccountId,
    /// REQ_UNLOCK = 0 | REQ_SLASH = 1 | REQ_COLLECT = 2
    pub req_type: u8,
    pub request_id: u64,
    #[ink(topic)]
    pub subject: Hash,
    pub vote_count: u32,
}

/// One miner's fees settled onto the vault's books (per entry of a
/// vote_collect_fees_batch quorum). `shortfall` > 0 means the bond couldn't
/// cover the owed delta (eaten by a slash) and the difference was written off.
#[ink::event]
pub struct FeesSettled {
    #[ink(topic)]
    pub miner: AccountId,
    pub collected: u128,
    pub new_cumulative: u128,
    pub shortfall: u128,
}

/// Slash applied on quorum. `seized = min(penalty, collateral)`;
/// `reimbursed + surplus == seized`.
#[ink::event]
pub struct MinerSlashed {
    #[ink(topic)]
    pub miner: AccountId,
    #[ink(topic)]
    pub swap_ref: Hash,
    pub seized: u128,
    pub reimbursed: u128,
    pub surplus: u128,
}

/// Direct reimbursement transfer failed; parked for `claim_slash`
#[ink::event]
pub struct SlashPending {
    #[ink(topic)]
    pub swap_ref: Hash,
    #[ink(topic)]
    pub user: AccountId,
    pub amount: u128,
}

/// User claimed a parked reimbursement
#[ink::event]
pub struct SlashClaimed {
    #[ink(topic)]
    pub swap_ref: Hash,
    #[ink(topic)]
    pub user: AccountId,
    pub amount: u128,
}

/// Fees recycled into the SN7 pool via add_stake_recycle. `tao_amount` is the
/// whole drained pot; `donated` is the share of it that arrived as unattributed
/// TAO rather than as settled protocol fees.
#[ink::event]
pub struct FeesRecycled {
    pub tao_amount: u128,
    pub donated: u128,
}

/// A unanimous round approved this candidate; they join the set only once they
/// call `accept_validator` themselves, proving they hold the key.
#[ink::event]
pub struct ValidatorPending {
    #[ink(topic)]
    pub candidate: AccountId,
}

/// Validator joined (after accepting) or was removed
#[ink::event]
pub struct ValidatorUpdated {
    #[ink(topic)]
    pub validator: AccountId,
    pub registered: bool,
}

/// Whole config replaced by a unanimous `vote_set_config` round. Carries every
/// field, not a delta — the round agreed on this exact tuple.
#[ink::event]
pub struct ConfigUpdated {
    pub min_collateral: u128,
    pub max_collateral: u128,
    pub consensus_threshold_percent: u8,
    pub vote_round_ttl: u32,
}

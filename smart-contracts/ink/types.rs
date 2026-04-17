use ink::prelude::string::String;
use ink::primitives::{AccountId, Hash};
use scale::{Decode, Encode};

type Balance = u128;

/// Status of a swap in its lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "std", derive(scale_info::TypeInfo, ink::storage::traits::StorageLayout))]
pub enum SwapStatus {
    Active,
    Fulfilled,
    Completed,
    TimedOut,
}

/// Type of validator vote on a swap
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "std", derive(scale_info::TypeInfo))]
#[repr(u8)]
pub enum VoteType {
    Confirm = 0,
    Timeout = 1,
    ExtendTimeout = 2,
}

/// Confirmed reservation for a miner after validator quorum.
///
/// Replaces the six per-miner reservation_* / miner_reserved_until Mappings.
/// `from_addr` stays here (not stripped) because the contract needs it on
/// expiry to strike against the user's source address (see `vote_reserve`
/// lazy-strike path). All six fields are read, written, and cleared together.
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "std", derive(scale_info::TypeInfo, ink::storage::traits::StorageLayout))]
pub struct Reservation {
    pub hash: Hash,
    pub from_addr: String,
    pub tao_amount: Balance,
    pub from_amount: Balance,
    pub to_amount: Balance,
    pub reserved_until: u32,
}

/// Full swap data stored on-chain
///
/// Rate and miner source address are snapshotted from the miner's commitment
/// at initiation time, so verification never depends on the miner remaining registered.
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "std", derive(scale_info::TypeInfo, ink::storage::traits::StorageLayout))]
pub struct SwapData {
    pub id: u64,
    pub user: AccountId,
    pub miner: AccountId,
    pub from_chain: String,
    pub to_chain: String,
    pub from_amount: Balance,
    pub to_amount: Balance,
    pub tao_amount: Balance,
    pub user_from_address: String,
    pub user_to_address: String,
    pub miner_from_address: String,
    pub miner_to_address: String,
    pub rate: String,
    pub from_tx_hash: String,
    pub from_tx_block: u32,
    pub to_tx_hash: String,
    pub to_tx_block: u32,
    pub status: SwapStatus,
    pub initiated_block: u32,
    pub timeout_block: u32,
    pub fulfilled_block: u32,
    pub completed_block: u32,
}

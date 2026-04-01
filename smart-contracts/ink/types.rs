use ink::prelude::string::String;
use ink::primitives::AccountId;
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
    pub source_chain: String,
    pub dest_chain: String,
    pub source_amount: Balance,
    pub dest_amount: Balance,
    pub tao_amount: Balance,
    pub user_source_address: String,
    pub user_dest_address: String,
    pub miner_source_address: String,
    pub miner_dest_address: String,
    pub rate: String,
    pub source_tx_hash: String,
    pub source_tx_block: u32,
    pub dest_tx_hash: String,
    pub dest_tx_block: u32,
    pub status: SwapStatus,
    pub initiated_block: u32,
    pub timeout_block: u32,
    pub fulfilled_block: u32,
    pub completed_block: u32,
}

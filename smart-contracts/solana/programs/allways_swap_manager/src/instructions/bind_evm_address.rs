use anchor_lang::prelude::*;
use solana_keccak_hasher::hashv;
use solana_secp256k1_recover::secp256k1_recover;

use crate::constants::{EVM_ADDR_BIND_SEED, EVM_BIND_SEED};
use crate::error::ErrorCode;
use crate::events::EvmAddressBound;
use crate::state::{EvmAddressBinding, EvmBinding};

/// secp256k1 group order n, halved: an s above it is the malleable twin of a valid signature.
const SECP256K1_HALF_N: [u8; 32] = [
    0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, //
    0x5d, 0x57, 0x6e, 0x73, 0x57, 0xa4, 0x50, 0x1d, 0xdf, 0xe9, 0x2f, 0x46, 0x68, 0x1b, 0x20, 0xa0,
];

/// A miner names the EVM address holding its Base bond; the vault's `lockBond(solanaPubkey)` names it
/// back, and validators attest only when both agree. Set-once both ways like `bind_hotkey` (V-M1): a
/// rebind could move a seizure mid-swap. The EVM key's signature, not collateral, prices the claim.
#[derive(Accounts)]
#[instruction(evm_address: [u8; 20])]
pub struct BindEvmAddress<'info> {
    #[account(mut)]
    pub miner: Signer<'info>,

    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + EvmBinding::INIT_SPACE,
        seeds = [EVM_BIND_SEED, miner.key().as_ref()],
        bump,
    )]
    pub evm_binding: Account<'info, EvmBinding>,

    #[account(
        init_if_needed,
        payer = miner,
        space = 8 + EvmAddressBinding::INIT_SPACE,
        seeds = [EVM_ADDR_BIND_SEED, evm_address.as_ref()],
        bump,
    )]
    pub evm_address_binding: Account<'info, EvmAddressBinding>,

    pub system_program: Program<'info, System>,
}

pub fn handler(ctx: Context<BindEvmAddress>, evm_address: [u8; 20], evm_signature: [u8; 65]) -> Result<()> {
    let signer = ctx.accounts.miner.key();
    // The zero address pads to the zero bond key, which the verdict reads as "never bound".
    require!(evm_address != [0u8; 20], ErrorCode::EvmAddressZero);
    verify_evm_signature(&evm_address, &signer, &evm_signature)?;

    let marker = &mut ctx.accounts.evm_address_binding;
    if marker.miner == Pubkey::default() {
        marker.miner = signer;
        marker.bump = ctx.bumps.evm_address_binding;
    } else {
        require!(marker.miner == signer, ErrorCode::EvmAddressAlreadyBound);
    }

    let binding = &mut ctx.accounts.evm_binding;
    if binding.miner != Pubkey::default() {
        // Same-address re-call is a no-op, so a retried bind tx can't fail a scripted onboarding.
        require!(binding.evm_address == evm_address, ErrorCode::EvmAddressChangeForbidden);
        return Ok(());
    }
    let now = Clock::get()?.unix_timestamp;
    binding.miner = signer;
    binding.evm_address = evm_address;
    binding.bound_at = now;
    binding.bump = ctx.bumps.evm_binding;

    emit!(EvmAddressBound { miner: signer, evm_address, bound_at: now });
    Ok(())
}

/// The text the EVM key personal_signs. Naming the Solana pubkey stops another signer replaying it;
/// naming the program stops a replay against another deployment. Mirrored in allways/solana/client.py.
pub fn evm_bind_message(evm_address: &[u8; 20], miner: &Pubkey, program_id: &Pubkey) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let hex: String =
        evm_address.iter().flat_map(|b| [HEX[(b >> 4) as usize], HEX[(b & 0xf) as usize]]).map(char::from).collect();
    format!("allways: bind EVM address 0x{hex} to Solana {miner} (program {program_id})")
}

/// EIP-191 personal_sign digest: keccak("\x19Ethereum Signed Message:\n" || decimal length || message).
pub fn evm_bind_digest(message: &str) -> [u8; 32] {
    let len = message.len().to_string();
    hashv(&[b"\x19Ethereum Signed Message:\n", len.as_bytes(), message.as_bytes()]).to_bytes()
}

/// `signature` is r || s || v with v in {27, 28}, as wallets return it; high-s is refused.
fn verify_evm_signature(evm_address: &[u8; 20], miner: &Pubkey, signature: &[u8; 65]) -> Result<()> {
    let v = signature[64];
    require!(v == 27 || v == 28, ErrorCode::EvmSignatureMalformed);
    require!(signature[32..64] <= SECP256K1_HALF_N[..], ErrorCode::EvmSignatureMalformed);
    let digest = evm_bind_digest(&evm_bind_message(evm_address, miner, &crate::ID));
    let pubkey =
        secp256k1_recover(&digest, v - 27, &signature[..64]).map_err(|_| error!(ErrorCode::EvmSignatureMalformed))?;
    let hashed = hashv(&[&pubkey.to_bytes()]).to_bytes();
    require!(hashed[12..] == evm_address[..], ErrorCode::EvmSignatureMismatch);
    Ok(())
}

/// The 32-byte bond key naming a Base bond (the vault keys bonds by miner address): the EVM address
/// left-padded, exactly as `abi.encode(address)` lays it out.
pub fn evm_bond_key(evm_address: &[u8; 20]) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[12..].copy_from_slice(evm_address);
    key
}

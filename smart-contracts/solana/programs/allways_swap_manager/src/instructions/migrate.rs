//! v3 upgrade cranks. A v10 account is too short AND laid out differently, so the live types can't
//! parse it: each crank reads it through a frozen mirror of the OLD layout, rebuilds it in the new one,
//! and grows the allocation. Safe to re-run — gated on a marker the crank itself moves, moving no funds.

use anchor_lang::prelude::*;
use anchor_lang::Discriminator;

use crate::constants::{
    ATTEST_MAX_AGE_SECS, BACKING_BIT_SOL, CONFIG_SEED, CONFIG_VERSION, MINER_SEED,
    SETTLEMENT_GRACE_SECS, TAO_MAX_SWAP_AMOUNT_RAO, TAO_MIN_COLLATERAL_RAO, TAO_MIN_SWAP_AMOUNT_RAO,
};
use crate::error::ErrorCode;
use crate::state::{Config, MinerState, ValidatorInfo};

/// The last deployed schema version — the only one these cranks accept as input.
const MIGRATE_FROM_VERSION: u32 = 10;
/// Byte offset of `Config.version` (discriminator + `admin`). Stable in every version, which is what
/// lets the crank read the marker before committing to a layout.
const CONFIG_VERSION_OFFSET: usize = 8 + 32;

/// `Config` as the v10 program wrote it: everything W1 and W2 added is absent. Field order is the
/// contract — do not reorder to match the live struct.
#[derive(AnchorDeserialize)]
struct ConfigV10 {
    admin: Pubkey,
    version: u32,
    min_collateral: u64,
    max_collateral: u64,
    fulfillment_timeout_secs: i64,
    min_swap_amount: u64,
    max_swap_amount: u64,
    reservation_ttl_secs: i64,
    consensus_threshold_percent: u8,
    validators: Vec<ValidatorInfo>,
    last_weights_update: i64,
    halted: bool,
    reservation_fee_lamports: u64,
    pool_window_secs: i64,
    finalize_window_secs: i64,
    weights_update_min_interval_secs: i64,
    max_total_extension_secs: i64,
    bump: u8,
}

/// `MinerState` as the v10 program wrote it: no `active_backings`, no `settling_until`.
#[derive(AnchorDeserialize)]
struct MinerStateV10 {
    miner: Pubkey,
    collateral: u64,
    active: bool,
    has_active_swap: bool,
    busy_until: i64,
    deactivation_at: i64,
    successful_swaps: u32,
    failed_swaps: u32,
    bump: u8,
}

#[derive(Accounts)]
pub struct MigrateConfig<'info> {
    /// Checked against the `admin` stored in the account's raw bytes — the live `Config` type can't
    /// parse a v10 account, so Anchor's `has_one` isn't available here.
    #[account(mut)]
    pub admin: Signer<'info>,

    /// CHECK: owner + discriminator + stored admin are all verified in the handler.
    #[account(mut, seeds = [CONFIG_SEED], bump)]
    pub config: UncheckedAccount<'info>,

    pub system_program: Program<'info, System>,
}

pub fn migrate_config(ctx: Context<MigrateConfig>) -> Result<()> {
    let info = ctx.accounts.config.to_account_info();
    let legacy = {
        let data = info.try_borrow_data()?;
        require!(
            info.owner == &crate::ID && data.len() >= CONFIG_VERSION_OFFSET + 4,
            ErrorCode::InvalidAccountForMigration
        );
        require!(
            data[..8] == Config::DISCRIMINATOR[..],
            ErrorCode::InvalidAccountForMigration
        );
        require!(
            data[8..40] == ctx.accounts.admin.key().to_bytes()[..],
            ErrorCode::NotMiner
        );

        let version =
            u32::from_le_bytes(data[CONFIG_VERSION_OFFSET..CONFIG_VERSION_OFFSET + 4].try_into().unwrap());
        if version == CONFIG_VERSION {
            msg!("config already at v{}", CONFIG_VERSION);
            return Ok(());
        }
        require!(
            version == MIGRATE_FROM_VERSION,
            ErrorCode::InvalidAccountForMigration
        );
        let legacy = ConfigV10::deserialize(&mut &data[8..])?;
        // The offset read and the parsed field must agree, or the layout mirror above has drifted.
        require!(legacy.version == version, ErrorCode::InvalidAccountForMigration);
        legacy
    };

    let migrated = Config {
        admin: legacy.admin,
        version: CONFIG_VERSION,
        min_collateral: legacy.min_collateral,
        max_collateral: legacy.max_collateral,
        fulfillment_timeout_secs: legacy.fulfillment_timeout_secs,
        min_swap_amount: legacy.min_swap_amount,
        max_swap_amount: legacy.max_swap_amount,
        // W1 + W2 additions, seeded from the same constants `initialize` uses. The TAO bounds and
        // collateral floor are deliberately inert defaults; the admin sets policy before the first
        // TAO-backed quote. The heartbeat starts unset, so the fuse is closed until relayers prove live.
        tao_min_swap_amount: TAO_MIN_SWAP_AMOUNT_RAO,
        tao_max_swap_amount: TAO_MAX_SWAP_AMOUNT_RAO,
        tao_min_collateral: TAO_MIN_COLLATERAL_RAO,
        settlement_grace_secs: SETTLEMENT_GRACE_SECS,
        last_attest_heartbeat: 0,
        attest_max_age_secs: ATTEST_MAX_AGE_SECS,
        reservation_ttl_secs: legacy.reservation_ttl_secs,
        consensus_threshold_percent: legacy.consensus_threshold_percent,
        validators: legacy.validators,
        last_weights_update: legacy.last_weights_update,
        halted: legacy.halted,
        reservation_fee_lamports: legacy.reservation_fee_lamports,
        pool_window_secs: legacy.pool_window_secs,
        finalize_window_secs: legacy.finalize_window_secs,
        weights_update_min_interval_secs: legacy.weights_update_min_interval_secs,
        max_total_extension_secs: legacy.max_total_extension_secs,
        bump: legacy.bump,
    };

    grow_to(
        &info,
        8 + Config::INIT_SPACE,
        &ctx.accounts.admin,
        &ctx.accounts.system_program,
    )?;
    write_account(&info, &migrated)?;
    msg!("config migrated v{} -> v{}", MIGRATE_FROM_VERSION, CONFIG_VERSION);
    Ok(())
}

#[derive(Accounts)]
pub struct MigrateMinerState<'info> {
    #[account(mut)]
    pub admin: Signer<'info>,

    /// Parsed as the LIVE type, so this crank can only run once `migrate_config` has already landed —
    /// the ordering the runbook needs, enforced by the account resolver rather than by convention.
    #[account(seeds = [CONFIG_SEED], bump = config.bump, has_one = admin)]
    pub config: Account<'info, Config>,

    /// CHECK: identified by address only; bound via the miner_state PDA seeds.
    pub miner: UncheckedAccount<'info>,

    /// CHECK: owner + discriminator verified in the handler; seeds bind it to `miner`.
    #[account(mut, seeds = [MINER_SEED, miner.key().as_ref()], bump)]
    pub miner_state: UncheckedAccount<'info>,

    pub system_program: Program<'info, System>,
}

pub fn migrate_miner_state(ctx: Context<MigrateMinerState>) -> Result<()> {
    let info = ctx.accounts.miner_state.to_account_info();
    let target_len = 8 + MinerState::INIT_SPACE;

    let legacy = {
        let data = info.try_borrow_data()?;
        require!(
            info.owner == &crate::ID && data.len() >= 8,
            ErrorCode::InvalidAccountForMigration
        );
        require!(
            data[..8] == MinerState::DISCRIMINATOR[..],
            ErrorCode::InvalidAccountForMigration
        );
        // MinerState carries no version, so the allocation length is the marker: only a migrated
        // account is this long.
        if data.len() >= target_len {
            msg!("miner state already migrated: {}", ctx.accounts.miner.key());
            return Ok(());
        }
        MinerStateV10::deserialize(&mut &data[8..])?
    };

    let migrated = MinerState {
        miner: legacy.miner,
        collateral: legacy.collateral,
        active: legacy.active,
        // The legacy bool WAS the SOL purse's activation — carry it across as that bit so the OR view
        // is unchanged for every miner and no re-activation vote is needed at the upgrade.
        active_backings: if legacy.active { BACKING_BIT_SOL } else { 0 },
        has_active_swap: legacy.has_active_swap,
        busy_until: legacy.busy_until,
        settling_until: 0,
        deactivation_at: legacy.deactivation_at,
        successful_swaps: legacy.successful_swaps,
        failed_swaps: legacy.failed_swaps,
        bump: legacy.bump,
    };

    grow_to(&info, target_len, &ctx.accounts.admin, &ctx.accounts.system_program)?;
    write_account(&info, &migrated)?;
    msg!("miner state migrated: {}", ctx.accounts.miner.key());
    Ok(())
}

/// Grow a program-owned account to `new_len`, topping up rent exemption from `payer` first (an account
/// left below the rent floor mid-migration would be reaped).
fn grow_to<'info>(
    info: &AccountInfo<'info>,
    new_len: usize,
    payer: &Signer<'info>,
    system_program: &Program<'info, System>,
) -> Result<()> {
    if info.data_len() >= new_len {
        return Ok(());
    }
    let needed = Rent::get()?
        .minimum_balance(new_len)
        .saturating_sub(info.lamports());
    if needed > 0 {
        anchor_lang::system_program::transfer(
            CpiContext::new(
                system_program.key(),
                anchor_lang::system_program::Transfer {
                    from: payer.to_account_info(),
                    to: info.clone(),
                },
            ),
            needed,
        )?;
    }
    info.resize(new_len)?;
    Ok(())
}

/// Overwrite an account body with `value` (discriminator included), leaving any trailing slack alone.
fn write_account<T: AccountSerialize>(info: &AccountInfo, value: &T) -> Result<()> {
    let mut data = info.try_borrow_mut_data()?;
    let mut cursor = std::io::Cursor::new(&mut data[..]);
    value.try_serialize(&mut cursor)?;
    Ok(())
}

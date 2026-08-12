//! Upgrade cranks. A legacy account is shorter AND laid out differently, so the live types can't
//! parse it: each crank reads it through a frozen mirror of the OLD layout, rebuilds it in the new one,
//! and grows the allocation. Safe to re-run — gated on a marker the crank itself moves, moving no funds.
//! v14 accepts BOTH from-versions: v10 (mainnet, never took v3) and v13 (testnet, v3 deployed).

use anchor_lang::prelude::*;
use anchor_lang::Discriminator;

use crate::constants::{
    ATTEST_MAX_AGE_SECS, BACKING_BIT_SOL, CONFIG_SEED, CONFIG_VERSION, MAX_BACKING_SLOTS,
    MINER_SEED, SETTLEMENT_GRACE_SECS, TAO_MAX_SWAP_AMOUNT_RAO, TAO_MIN_COLLATERAL_RAO,
    TAO_MIN_SWAP_AMOUNT_RAO,
};
use crate::error::ErrorCode;
use crate::state::{Config, MinerState, ValidatorInfo};

/// The pre-v3 deployed schema — the oldest input these cranks accept.
const MIGRATE_FROM_V10: u32 = 10;
/// The v3 split-collateral schema — same `Config` layout as v14 (only `MinerState` grew), so its
/// config crank is a version stamp and its miner-state crank a layout grow.
const MIGRATE_FROM_V13: u32 = 13;
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
#[derive(AnchorDeserialize, InitSpace)]
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

/// `MinerState` as the v13 program wrote it: global scalar locks, no per-hub arrays. Field order is
/// the contract — do not reorder to match the live struct.
#[derive(AnchorDeserialize, InitSpace)]
struct MinerStateV13 {
    miner: Pubkey,
    collateral: u64,
    active: bool,
    active_backings: u8,
    has_active_swap: bool,
    busy_until: i64,
    settling_until: i64,
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

/// The two accepted input shapes, decided by the stored version marker.
enum LegacyConfig {
    V10(ConfigV10),
    /// v13's `Config` layout is identical to v14's — parsed with the live type.
    V13(Config),
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
        if version == MIGRATE_FROM_V13 {
            LegacyConfig::V13(Config::deserialize(&mut &data[8..])?)
        } else {
            require!(
                version == MIGRATE_FROM_V10,
                ErrorCode::InvalidAccountForMigration
            );
            let legacy = ConfigV10::deserialize(&mut &data[8..])?;
            // The offset read and the parsed field must agree, or the layout mirror above has drifted.
            require!(legacy.version == version, ErrorCode::InvalidAccountForMigration);
            LegacyConfig::V10(legacy)
        }
    };

    let from_version = match &legacy {
        LegacyConfig::V10(_) => MIGRATE_FROM_V10,
        LegacyConfig::V13(_) => MIGRATE_FROM_V13,
    };
    let legacy = match legacy {
        LegacyConfig::V13(mut cfg) => {
            cfg.version = CONFIG_VERSION;
            write_account(&info, &cfg)?;
            msg!("config migrated v{} -> v{}", from_version, CONFIG_VERSION);
            return Ok(());
        }
        LegacyConfig::V10(legacy) => legacy,
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
    msg!("config migrated v{} -> v{}", from_version, CONFIG_VERSION);
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

/// Broadcast a global scalar lock into every per-hub slot — conservative: the max view is identical,
/// and each hub honors the old global lock until it expires.
fn broadcast(v: i64) -> [i64; MAX_BACKING_SLOTS] {
    [v; MAX_BACKING_SLOTS]
}

pub fn migrate_miner_state(ctx: Context<MigrateMinerState>) -> Result<()> {
    let info = ctx.accounts.miner_state.to_account_info();
    let target_len = 8 + MinerState::INIT_SPACE;
    const V10_LEN: usize = 8 + MinerStateV10::INIT_SPACE;
    const V13_LEN: usize = 8 + MinerStateV13::INIT_SPACE;

    let migrated = {
        let data = info.try_borrow_data()?;
        require!(
            info.owner == &crate::ID && data.len() >= 8,
            ErrorCode::InvalidAccountForMigration
        );
        require!(
            data[..8] == MinerState::DISCRIMINATOR[..],
            ErrorCode::InvalidAccountForMigration
        );
        // MinerState carries no version, so the allocation length is the marker — it names the source
        // layout exactly (v10 and v13 differ by 9 bytes; only a migrated account reaches target_len).
        if data.len() >= target_len {
            msg!("miner state already migrated: {}", ctx.accounts.miner.key());
            return Ok(());
        }
        if data.len() == V13_LEN {
            let legacy = MinerStateV13::deserialize(&mut &data[8..])?;
            MinerState {
                miner: legacy.miner,
                collateral: legacy.collateral,
                active: legacy.active,
                active_backings: legacy.active_backings,
                has_active_swap: legacy.has_active_swap,
                // The runbook drains before upgrading, so this is ~always 0. If a swap somehow
                // survives, every active hub is marked in-flight — conservative, never permissive.
                active_swap_backings: if legacy.has_active_swap { legacy.active_backings } else { 0 },
                busy_until: broadcast(legacy.busy_until),
                settling_until: broadcast(legacy.settling_until),
                reserved_collateral: [0; MAX_BACKING_SLOTS],
                deactivation_at: legacy.deactivation_at,
                successful_swaps: legacy.successful_swaps,
                failed_swaps: legacy.failed_swaps,
                bump: legacy.bump,
            }
        } else if data.len() == V10_LEN {
            let legacy = MinerStateV10::deserialize(&mut &data[8..])?;
            MinerState {
                miner: legacy.miner,
                collateral: legacy.collateral,
                active: legacy.active,
                // The legacy bool WAS the SOL purse's activation — carry it across as that bit so the
                // OR view is unchanged for every miner and no re-activation vote is needed.
                active_backings: if legacy.active { BACKING_BIT_SOL } else { 0 },
                has_active_swap: legacy.has_active_swap,
                active_swap_backings: if legacy.has_active_swap { BACKING_BIT_SOL } else { 0 },
                busy_until: broadcast(legacy.busy_until),
                settling_until: [0; MAX_BACKING_SLOTS],
                reserved_collateral: [0; MAX_BACKING_SLOTS],
                deactivation_at: legacy.deactivation_at,
                successful_swaps: legacy.successful_swaps,
                failed_swaps: legacy.failed_swaps,
                bump: legacy.bump,
            }
        } else {
            return err!(ErrorCode::InvalidAccountForMigration);
        }
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

//! Upgrade cranks. A legacy account is shorter AND laid out differently, so the live types can't
//! parse it: each crank reads it through a frozen mirror of the OLD layout, rebuilds it in the new one,
//! and grows the allocation. Safe to re-run — gated on a marker the crank itself moves, moving no funds.
//! v17 accepts five from-versions: v10 (pre-v3), v13/v14 (one layout), and v15/v16 (one layout). Each
//! is first normalized to the v16 shape, then grown into v17, which only appends fields.

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
/// The v3 split-collateral schema — same `Config` layout as v14 (only `MinerState` grew).
const MIGRATE_FROM_V13: u32 = 13;
/// v14 is the per-hub MinerState schema; v15 appends `vault_generation`.
const MIGRATE_FROM_V14: u32 = 14;
/// v15 and v16 share a layout (v16 changed fee semantics, not fields).
const MIGRATE_FROM_V15: u32 = 15;
const MIGRATE_FROM_V16: u32 = 16;
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
    /// parse a legacy account, so Anchor's `has_one` isn't available here.
    #[account(mut)]
    pub admin: Signer<'info>,

    /// CHECK: owner + discriminator + stored admin are all verified in the handler.
    #[account(mut, seeds = [CONFIG_SEED], bump)]
    pub config: UncheckedAccount<'info>,

    pub system_program: Program<'info, System>,
}

/// `Config` as the v13 AND v14 programs wrote it — the two share a layout. Field order is the contract.
#[derive(AnchorDeserialize)]
struct ConfigV14 {
    admin: Pubkey,
    version: u32,
    min_collateral: u64,
    max_collateral: u64,
    fulfillment_timeout_secs: i64,
    min_swap_amount: u64,
    max_swap_amount: u64,
    tao_min_swap_amount: u64,
    tao_max_swap_amount: u64,
    tao_min_collateral: u64,
    settlement_grace_secs: i64,
    last_attest_heartbeat: i64,
    attest_max_age_secs: i64,
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

/// `Config` as the v15 AND v16 programs wrote it: v14 plus a trailing scalar `vault_generation`, with
/// one global heartbeat/max-age/generation. Field order is the contract.
#[derive(AnchorDeserialize, AnchorSerialize)]
pub(crate) struct ConfigV16 {
    pub admin: Pubkey,
    pub version: u32,
    pub min_collateral: u64,
    pub max_collateral: u64,
    pub fulfillment_timeout_secs: i64,
    pub min_swap_amount: u64,
    pub max_swap_amount: u64,
    pub tao_min_swap_amount: u64,
    pub tao_max_swap_amount: u64,
    pub tao_min_collateral: u64,
    pub settlement_grace_secs: i64,
    pub last_attest_heartbeat: i64,
    pub attest_max_age_secs: i64,
    pub reservation_ttl_secs: i64,
    pub consensus_threshold_percent: u8,
    pub validators: Vec<ValidatorInfo>,
    pub last_weights_update: i64,
    pub halted: bool,
    pub reservation_fee_lamports: u64,
    pub pool_window_secs: i64,
    pub finalize_window_secs: i64,
    pub weights_update_min_interval_secs: i64,
    pub max_total_extension_secs: i64,
    pub bump: u8,
    pub vault_generation: u64,
}

impl From<ConfigV14> for ConfigV16 {
    /// Generation 0 is correct: the epochs already attested came from the vault in service then.
    fn from(c: ConfigV14) -> Self {
        ConfigV16 {
            admin: c.admin,
            version: c.version,
            min_collateral: c.min_collateral,
            max_collateral: c.max_collateral,
            fulfillment_timeout_secs: c.fulfillment_timeout_secs,
            min_swap_amount: c.min_swap_amount,
            max_swap_amount: c.max_swap_amount,
            tao_min_swap_amount: c.tao_min_swap_amount,
            tao_max_swap_amount: c.tao_max_swap_amount,
            tao_min_collateral: c.tao_min_collateral,
            settlement_grace_secs: c.settlement_grace_secs,
            last_attest_heartbeat: c.last_attest_heartbeat,
            attest_max_age_secs: c.attest_max_age_secs,
            reservation_ttl_secs: c.reservation_ttl_secs,
            consensus_threshold_percent: c.consensus_threshold_percent,
            validators: c.validators,
            last_weights_update: c.last_weights_update,
            halted: c.halted,
            reservation_fee_lamports: c.reservation_fee_lamports,
            pool_window_secs: c.pool_window_secs,
            finalize_window_secs: c.finalize_window_secs,
            weights_update_min_interval_secs: c.weights_update_min_interval_secs,
            max_total_extension_secs: c.max_total_extension_secs,
            bump: c.bump,
            vault_generation: 0,
        }
    }
}

impl From<ConfigV10> for ConfigV16 {
    /// W1 + W2 additions, seeded from the same constants `initialize` uses. The heartbeat starts unset,
    /// so the TAO fuse is closed until relayers prove live.
    fn from(c: ConfigV10) -> Self {
        ConfigV16 {
            admin: c.admin,
            version: c.version,
            min_collateral: c.min_collateral,
            max_collateral: c.max_collateral,
            fulfillment_timeout_secs: c.fulfillment_timeout_secs,
            min_swap_amount: c.min_swap_amount,
            max_swap_amount: c.max_swap_amount,
            tao_min_swap_amount: TAO_MIN_SWAP_AMOUNT_RAO,
            tao_max_swap_amount: TAO_MAX_SWAP_AMOUNT_RAO,
            tao_min_collateral: TAO_MIN_COLLATERAL_RAO,
            settlement_grace_secs: SETTLEMENT_GRACE_SECS,
            last_attest_heartbeat: 0,
            attest_max_age_secs: ATTEST_MAX_AGE_SECS,
            reservation_ttl_secs: c.reservation_ttl_secs,
            consensus_threshold_percent: c.consensus_threshold_percent,
            validators: c.validators,
            last_weights_update: c.last_weights_update,
            halted: c.halted,
            reservation_fee_lamports: c.reservation_fee_lamports,
            pool_window_secs: c.pool_window_secs,
            finalize_window_secs: c.finalize_window_secs,
            weights_update_min_interval_secs: c.weights_update_min_interval_secs,
            max_total_extension_secs: c.max_total_extension_secs,
            bump: c.bump,
            vault_generation: 0,
        }
    }
}

/// v16 → v17 is append-only: every existing field keeps its offset (the scalars ARE the TAO hub's
/// fuse), and the Base hub's six fields land after `vault_generation` at 0 — closed and fused shut.
pub(crate) fn to_v17(c: ConfigV16) -> Config {
    Config {
        admin: c.admin,
        version: CONFIG_VERSION,
        min_collateral: c.min_collateral,
        max_collateral: c.max_collateral,
        fulfillment_timeout_secs: c.fulfillment_timeout_secs,
        min_swap_amount: c.min_swap_amount,
        max_swap_amount: c.max_swap_amount,
        tao_min_swap_amount: c.tao_min_swap_amount,
        tao_max_swap_amount: c.tao_max_swap_amount,
        tao_min_collateral: c.tao_min_collateral,
        settlement_grace_secs: c.settlement_grace_secs,
        last_attest_heartbeat: c.last_attest_heartbeat,
        attest_max_age_secs: c.attest_max_age_secs,
        reservation_ttl_secs: c.reservation_ttl_secs,
        consensus_threshold_percent: c.consensus_threshold_percent,
        validators: c.validators,
        last_weights_update: c.last_weights_update,
        halted: c.halted,
        reservation_fee_lamports: c.reservation_fee_lamports,
        pool_window_secs: c.pool_window_secs,
        finalize_window_secs: c.finalize_window_secs,
        weights_update_min_interval_secs: c.weights_update_min_interval_secs,
        max_total_extension_secs: c.max_total_extension_secs,
        bump: c.bump,
        vault_generation: c.vault_generation,
        usdc_min_swap_amount: 0,
        usdc_max_swap_amount: 0,
        usdc_min_collateral: 0,
        usdc_last_attest_heartbeat: 0,
        usdc_attest_max_age_secs: 0,
        usdc_vault_generation: 0,
    }
}

/// Parse a legacy Config body (bytes after the discriminator) into the v16 shape, by its version.
pub(crate) fn parse_legacy(version: u32, body: &[u8]) -> Result<ConfigV16> {
    let mut body = body;
    let cfg = match version {
        MIGRATE_FROM_V15 | MIGRATE_FROM_V16 => ConfigV16::deserialize(&mut body)?,
        MIGRATE_FROM_V13 | MIGRATE_FROM_V14 => ConfigV14::deserialize(&mut body)?.into(),
        MIGRATE_FROM_V10 => ConfigV10::deserialize(&mut body)?.into(),
        _ => return err!(ErrorCode::InvalidAccountForMigration),
    };
    // The offset read and the parsed field must agree, or a layout mirror above has drifted.
    require!(cfg.version == version, ErrorCode::InvalidAccountForMigration);
    Ok(cfg)
}

pub fn migrate_config(ctx: Context<MigrateConfig>) -> Result<()> {
    let info = ctx.accounts.config.to_account_info();
    let (from_version, migrated) = {
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
        (version, to_v17(parse_legacy(version, &data[8..])?))
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
    pub config: Box<Account<'info, Config>>,

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
            // Refuse a miner that still has an in-flight swap: a multi-hub v13 account has no way to
            // say WHICH hub the swap was on, so marking every active hub in-flight would brick the
            // sibling hub with a phantom busy bit. Surface the drain violation; drain then re-run.
            require!(!legacy.has_active_swap, ErrorCode::MigrationSwapNotDrained);
            MinerState {
                miner: legacy.miner,
                collateral: legacy.collateral,
                active: legacy.active,
                active_backings: legacy.active_backings,
                has_active_swap: false,
                active_swap_backings: 0,
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

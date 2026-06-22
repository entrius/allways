// Phase 9 — reservation lottery: open_or_request / resolve_pool, flat fee, guards (LiteSVM).
//   cargo test -p allways_swap_manager --test test_reservation
//
// The weighted draw itself is unit-tested as a pure fn in src/lottery.rs (LiteSVM doesn't populate
// SlotHashes with future slots, so resolve here uses the deterministic fallback seed). These tests
// cover the on-chain machinery: open pins the miner quote, the per-request fee accrues to treasury,
// validator dedup, pair-mismatch, window timing, guards, single/multi-requester resolve.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    allways_swap_manager::constants::{POOL_WINDOW_SECS, RESERVATION_FEE_LAMPORTS},
    allways_swap_manager::state::{MinerState, Pool, Reservation, Treasury},
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;
const SLOT_HASHES_ID: Pubkey = Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111");
const REQ_ACTIVATE: u8 = 0;
const BASE_TS: i64 = 1_700_000_000;
const TTL: i64 = 1_800;

// Default pinned quote (matches the pair the tests open on).
const MFROM: &str = "minerBTCaddr";
const MTO: &str = "minerSOLaddr";
const RATE: &str = "1.5";

fn pid() -> Pubkey {
    allways_swap_manager::id()
}
fn config_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"config"], &pid()).0
}
fn vault_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"vault"], &pid()).0
}
fn treasury_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"treasury"], &pid()).0
}
fn miner_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"miner", m.as_ref()], &pid()).0
}
fn vote_pda(req: u8, m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[req], m.as_ref()], &pid()).0
}
fn resv_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"resv", m.as_ref()], &pid()).0
}
fn quote_pda(m: &Pubkey, f: &str, t: &str) -> Pubkey {
    Pubkey::find_program_address(&[b"quote", m.as_ref(), f.as_bytes(), t.as_bytes()], &pid()).0
}
fn pool_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"pool", m.as_ref()], &pid()).0
}

fn set_clock(svm: &mut LiteSVM, ts: i64) {
    let mut clock = svm.get_sysvar::<Clock>();
    clock.unix_timestamp = ts;
    svm.set_sysvar::<Clock>(&clock);
}

fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    svm.expire_blockhash();
    let bh = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &bh);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn init_ix(admin: &Pubkey, min_swap: u64, max_swap: u64, ttl: i64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Initialize {
            min_collateral: 0,
            max_collateral: 0,
            fulfillment_timeout_secs: 100,
            consensus_threshold_percent: 66,
            min_swap_amount: min_swap,
            max_swap_amount: max_swap,
            reservation_ttl_secs: ttl,
        }
        .data(),
        allways_swap_manager::accounts::Initialize {
            admin: *admin,
            config: config_pda(),
            vault: vault_pda(),
            treasury: treasury_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn add_validator_ix(admin: &Pubkey, v: Pubkey, weight: u64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::AddValidator { validator: v, weight }.data(),
        allways_swap_manager::accounts::AdminConfig { admin: *admin, config: config_pda() }
            .to_account_metas(None),
    )
}
fn post_ix(miner: &Pubkey, amount: u64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::PostCollateral { amount }.data(),
        allways_swap_manager::accounts::PostCollateral {
            miner: *miner,
            config: config_pda(),
            miner_state: miner_pda(miner),
            vault: vault_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn vote_activate_ix(validator: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteActivate {}.data(),
        allways_swap_manager::accounts::VoteActivate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            vote_round: vote_pda(REQ_ACTIVATE, miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn set_quote_ix(miner: &Pubkey, f: &str, t: &str, mfrom: &str, mto: &str, rate: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetQuote {
            from_chain: f.to_string(),
            to_chain: t.to_string(),
            miner_from_addr: mfrom.to_string(),
            miner_to_addr: mto.to_string(),
            rate: rate.to_string(),
            liquidity: 1_000,
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *miner,
            quote: quote_pda(miner, f, t),
            treasury: treasury_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
#[allow(clippy::too_many_arguments)]
fn open_ix(
    validator: &Pubkey,
    miner: &Pubkey,
    f: &str,
    t: &str,
    user: &Pubkey,
    ufrom: &str,
    uto: &str,
    sol_amount: u64,
    from_amount: u128,
    to_amount: u128,
) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::OpenOrRequest {
            from_chain: f.to_string(),
            to_chain: t.to_string(),
            user: *user,
            user_from_addr: ufrom.to_string(),
            user_to_addr: uto.to_string(),
            sol_amount,
            from_amount,
            to_amount,
        }
        .data(),
        allways_swap_manager::accounts::OpenOrRequest {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            quote: quote_pda(miner, f, t),
            pool: pool_pda(miner),
            treasury: treasury_pda(),
            reservation: resv_pda(miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn resolve_ix(caller: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::ResolvePool {}.data(),
        allways_swap_manager::accounts::ResolvePool {
            caller: *caller,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            pool: pool_pda(miner),
            reservation: resv_pda(miner),
            slot_hashes: SLOT_HASHES_ID,
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn deactivate_ix(miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Deactivate {}.data(),
        allways_swap_manager::accounts::Deactivate {
            miner: *miner,
            miner_state: miner_pda(miner),
        }
        .to_account_metas(None),
    )
}

fn reservation(svm: &LiteSVM, miner: &Pubkey) -> Reservation {
    let a = svm.get_account(&resv_pda(miner)).unwrap();
    Reservation::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn pool(svm: &LiteSVM, miner: &Pubkey) -> Pool {
    let a = svm.get_account(&pool_pda(miner)).unwrap();
    Pool::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn treasury(svm: &LiteSVM) -> u64 {
    let a = svm.get_account(&treasury_pda()).unwrap();
    Treasury::try_deserialize(&mut a.data.as_slice()).unwrap().total
}
fn is_active(svm: &LiteSVM, miner: &Pubkey) -> bool {
    let a = svm.get_account(&miner_pda(miner)).unwrap();
    MinerState::try_deserialize(&mut a.data.as_slice()).unwrap().active
}

/// init + 3 validators (weight 1) + a funded, active miner with a BTC→SOL quote posted. Clock at
/// BASE_TS. Returns (svm, admin, validators, miner).
fn setup(min_swap: u64, max_swap: u64) -> (LiteSVM, Keypair, Vec<Keypair>, Keypair) {
    let mut svm = LiteSVM::new();
    svm.add_program(pid(), include_bytes!("../../../target/deploy/allways_swap_manager.so")).unwrap();
    set_clock(&mut svm, BASE_TS);

    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, init_ix(&admin.pubkey(), min_swap, max_swap, TTL), &admin.pubkey(), &admin).expect("init");

    let mut vals = Vec::new();
    for _ in 0..3 {
        let v = Keypair::new();
        svm.airdrop(&v.pubkey(), 100_000_000_000).unwrap();
        send(&mut svm, add_validator_ix(&admin.pubkey(), v.pubkey(), 1), &admin.pubkey(), &admin).expect("add val");
        vals.push(v);
    }

    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, post_ix(&miner.pubkey(), 10_000_000_000), &miner.pubkey(), &miner).expect("post");
    send(&mut svm, set_quote_ix(&miner.pubkey(), "BTC", "SOL", MFROM, MTO, RATE), &miner.pubkey(), &miner).expect("quote");
    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("a0");
    send(&mut svm, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("a1");
    (svm, admin, vals, miner)
}

#[test]
fn test_open_pins_quote_and_charges_fee() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    let t0 = treasury(&svm);

    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u1", "uSOL", 2_000_000_000, 100_000, 0), &vals[0].pubkey(), &vals[0]).expect("open");

    let p = pool(&svm, &miner.pubkey());
    assert_eq!(p.from_chain, "BTC");
    assert_eq!(p.to_chain, "SOL");
    assert_eq!(p.miner_from_addr, MFROM, "pinned from MinerQuote");
    assert_eq!(p.miner_to_addr, MTO);
    assert_eq!(p.rate, RATE);
    assert_eq!(p.opened_at, BASE_TS);
    assert_eq!(p.closes_at, BASE_TS + POOL_WINDOW_SECS);
    assert_eq!(p.requests.len(), 1);
    assert_eq!(p.requests[0].validator, vals[0].pubkey());
    assert_eq!(treasury(&svm), t0 + RESERVATION_FEE_LAMPORTS, "flat fee accrued to treasury");
}

#[test]
fn test_each_request_charges_fee() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    let t0 = treasury(&svm);
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u1", "uSOL", 1, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open");
    send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u2", "uSOL", 1, 1, 0), &vals[1].pubkey(), &vals[1]).expect("join");
    assert_eq!(treasury(&svm), t0 + 2 * RESERVATION_FEE_LAMPORTS, "both opener and joiner pay");
    assert_eq!(pool(&svm, &miner.pubkey()).requests.len(), 2);
}

#[test]
fn test_duplicate_validator_rejected() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u1", "uSOL", 1, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open");
    let dup = send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u1", "uSOL", 1, 1, 0), &vals[0].pubkey(), &vals[0]);
    assert!(dup.is_err(), "same validator twice in one pool must be rejected");
    assert_eq!(pool(&svm, &miner.pubkey()).requests.len(), 1);
}

#[test]
fn test_pair_mismatch_rejected() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    // Miner also quotes BTC→TAO so that quote account exists for the join attempt.
    send(&mut svm, set_quote_ix(&miner.pubkey(), "BTC", "TAO", MFROM, "minerTAOaddr", "2.0"), &miner.pubkey(), &miner).expect("quote2");
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u1", "uSOL", 1, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open BTC/SOL");
    let mism = send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "TAO", &user, "u2", "uTAO", 1, 1, 0), &vals[1].pubkey(), &vals[1]);
    assert!(mism.is_err(), "joining with a different pair than the pinned one must be rejected");
}

#[test]
fn test_single_requester_resolve_creates_reservation() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "userBTC", "userSOL", 2_000_000_000, 100_000, 0), &vals[0].pubkey(), &vals[0]).expect("open");

    // before close → cannot resolve
    let early = send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]);
    assert!(early.is_err(), "resolve before window close must fail");

    // warp past close, resolve (sole entrant wins regardless of seed)
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("resolve");

    let r = reservation(&svm, &miner.pubkey());
    assert_eq!(r.reserved_until, BASE_TS + POOL_WINDOW_SECS + 1 + TTL, "reserved with TTL from resolve time");
    assert_eq!(r.from_addr, "userBTC", "winner's user source addr");
    assert_eq!(r.from_chain, "BTC");
    assert_eq!(r.to_chain, "SOL");
    assert_eq!(r.sol_amount, 2_000_000_000);
    assert_eq!(r.miner_from_addr, MFROM, "pinned miner quote carried into reservation");
    assert_eq!(r.miner_to_addr, MTO);
    assert_eq!(r.rate, RATE);
    // pool reset for reuse
    assert_eq!(pool(&svm, &miner.pubkey()).opened_at, 0, "pool reset after resolve");
    assert!(pool(&svm, &miner.pubkey()).requests.is_empty());
}

#[test]
fn test_multi_requester_resolve_picks_one() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let u0 = Keypair::new().pubkey();
    let u1 = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &u0, "from0", "to0", 2_000_000_000, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open");
    send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "SOL", &u1, "from1", "to1", 2_000_000_000, 1, 0), &vals[1].pubkey(), &vals[1]).expect("join");

    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    send(&mut svm, resolve_ix(&vals[2].pubkey(), &miner.pubkey()), &vals[2].pubkey(), &vals[2]).expect("resolve");

    let r = reservation(&svm, &miner.pubkey());
    assert!(r.reserved_until > 0, "a winner was reserved");
    assert!(r.from_addr == "from0" || r.from_addr == "from1", "winner is one of the two entrants");
}

#[test]
fn test_resolve_empty_pool_fails() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    // never opened → NoRequests
    let r = send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]);
    assert!(r.is_err(), "resolving a never-opened pool must fail");
}

#[test]
fn test_join_after_close_fails() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u1", "uSOL", 1, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    let late = send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u2", "uSOL", 1, 1, 0), &vals[1].pubkey(), &vals[1]);
    assert!(late.is_err(), "joining after the window closed must fail (must resolve first)");
}

#[test]
fn test_amount_bounds() {
    let (mut svm, _admin, vals, miner) = setup(1_000_000_000, 5_000_000_000);
    let user = Keypair::new().pubkey();
    let below = send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u", "uSOL", 500_000_000, 1, 0), &vals[0].pubkey(), &vals[0]);
    assert!(below.is_err(), "below min rejected");
    let above = send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u", "uSOL", 9_000_000_000, 1, 0), &vals[0].pubkey(), &vals[0]);
    assert!(above.is_err(), "above max rejected");
}

#[test]
fn test_requires_active_miner() {
    let (mut svm, _admin, vals, _miner) = setup(0, 0);
    // an inactive miner that posted collateral + a quote but was never activated
    let inactive = Keypair::new();
    svm.airdrop(&inactive.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, post_ix(&inactive.pubkey(), 1_000_000_000), &inactive.pubkey(), &inactive).expect("post");
    send(&mut svm, set_quote_ix(&inactive.pubkey(), "BTC", "SOL", MFROM, MTO, RATE), &inactive.pubkey(), &inactive).expect("quote");
    let user = Keypair::new().pubkey();
    let res = send(&mut svm, open_ix(&vals[0].pubkey(), &inactive.pubkey(), "BTC", "SOL", &user, "u", "uSOL", 1, 1, 0), &vals[0].pubkey(), &vals[0]);
    assert!(res.is_err(), "inactive miner cannot be pooled");
}

#[test]
fn test_open_blocked_while_reserved() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u1", "uSOL", 2_000_000_000, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("resolve");
    assert!(reservation(&svm, &miner.pubkey()).reserved_until > 0);

    // a new open is blocked while the reservation is active
    let blocked = send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u2", "uSOL", 2_000_000_000, 1, 0), &vals[1].pubkey(), &vals[1]);
    assert!(blocked.is_err(), "cannot open a new pool while a reservation is active");
}

#[test]
fn test_open_pool_blocks_deactivate() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u1", "uSOL", 1, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open");
    assert_ne!(pool(&svm, &miner.pubkey()).opened_at, 0);

    // miner is busy the moment a pool opens (pre-resolve) — cannot self-deactivate
    let blocked = send(&mut svm, deactivate_ix(&miner.pubkey()), &miner.pubkey(), &miner);
    assert!(blocked.is_err(), "cannot self-deactivate while a pool is open");
}

#[test]
fn test_reservation_blocks_deactivate_until_expiry() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u", "uSOL", 2_000_000_000, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("resolve");
    assert!(is_active(&svm, &miner.pubkey()));

    let blocked = send(&mut svm, deactivate_ix(&miner.pubkey()), &miner.pubkey(), &miner);
    assert!(blocked.is_err(), "cannot self-deactivate while reserved");

    let resv_until = reservation(&svm, &miner.pubkey()).reserved_until;
    set_clock(&mut svm, resv_until + 1);
    send(&mut svm, deactivate_ix(&miner.pubkey()), &miner.pubkey(), &miner).expect("deactivate after expiry");
    assert!(!is_active(&svm, &miner.pubkey()), "miner deactivated once reservation expired");
}

// ---- Review-fix coverage (PR #484): over-collateral entry gate, busy-lock deactivation, bounds ----

const REQ_DEACTIVATE: u8 = 5;

fn vote_deactivate_ix(validator: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteDeactivate {}.data(),
        allways_swap_manager::accounts::VoteDeactivate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            vote_round: vote_pda(REQ_DEACTIVATE, miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn set_max_swap_ix(admin: &Pubkey, amount: u64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetMaxSwapAmount { amount }.data(),
        allways_swap_manager::accounts::AdminConfig { admin: *admin, config: config_pda() }.to_account_metas(None),
    )
}
fn set_min_swap_ix(admin: &Pubkey, amount: u64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetMinSwapAmount { amount }.data(),
        allways_swap_manager::accounts::AdminConfig { admin: *admin, config: config_pda() }.to_account_metas(None),
    )
}
fn busy_until(svm: &LiteSVM, miner: &Pubkey) -> i64 {
    let a = svm.get_account(&miner_pda(miner)).unwrap();
    MinerState::try_deserialize(&mut a.data.as_slice()).unwrap().busy_until
}
fn collateral(svm: &LiteSVM, miner: &Pubkey) -> u64 {
    let a = svm.get_account(&miner_pda(miner)).unwrap();
    MinerState::try_deserialize(&mut a.data.as_slice()).unwrap().collateral
}

#[test]
fn test_open_requires_overcollateralization() {
    // setup posts 10 SOL collateral; the requirement is 1.10×. A swap whose 1.10× need exceeds the
    // collateral must be rejected at open (review #1) so vote_initiate can't later strand a user.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    assert_eq!(collateral(&svm, &miner.pubkey()), 10_000_000_000);

    // 9.5 SOL × 1.10 = 10.45 SOL > 10 SOL → rejected.
    let too_big = send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u", "uSOL", 9_500_000_000, 1, 0), &vals[0].pubkey(), &vals[0]);
    assert!(too_big.is_err(), "swap needing >collateral at 1.10× must be rejected at open");

    // 9 SOL × 1.10 = 9.9 SOL ≤ 10 SOL → accepted.
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u", "uSOL", 9_000_000_000, 1, 0), &vals[0].pubkey(), &vals[0]).expect("affordable swap opens");
}

#[test]
fn test_cannot_force_deactivate_while_pooled() {
    // A miner with an open pool is busy and must not be force-deactivatable mid-window — deactivation
    // is only for idle miners (preserves the busy ⟹ active invariant; user req / review #3).
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u", "uSOL", 1, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open");
    let blocked = send(&mut svm, vote_deactivate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]);
    assert!(blocked.is_err(), "cannot force-deactivate a miner with an open pool");
    assert!(is_active(&svm, &miner.pubkey()), "miner stays active");
}

#[test]
fn test_cannot_force_deactivate_while_reserved() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL", &user, "u", "uSOL", 2_000_000_000, 1, 0), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("resolve");
    let blocked = send(&mut svm, vote_deactivate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]);
    assert!(blocked.is_err(), "cannot force-deactivate a reserved miner");
}
#[test]
fn test_swap_amount_bounds_cannot_be_contradictory() {
    // Admin cannot drive max_swap < min_swap (or vice-versa) — would brick open_or_request (review #6).
    let (mut svm, admin, _vals, _miner) = setup(1_000_000_000, 5_000_000_000);
    assert!(send(&mut svm, set_max_swap_ix(&admin.pubkey(), 500_000_000), &admin.pubkey(), &admin).is_err(), "max < min rejected");
    assert!(send(&mut svm, set_min_swap_ix(&admin.pubkey(), 6_000_000_000), &admin.pubkey(), &admin).is_err(), "min > max rejected");
    send(&mut svm, set_max_swap_ix(&admin.pubkey(), 8_000_000_000), &admin.pubkey(), &admin).expect("widening max is allowed");
}
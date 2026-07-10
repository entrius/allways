// Phase 9 — reservation lottery: open_or_request / resolve_pool, flat fee, guards (LiteSVM).
//   cargo test -p allways_swap_manager --test test_reservation
//
// resolve_pool is two-phase: the first call after the window shuts arms the draw on a future slot,
// a later call resolves against that slot's hash. Tests seed the real SlotHashes sysvar between the
// two (see `arm_and_resolve`) — there is no fallback seed to lean on. The weighted draw itself is
// unit-tested as a pure fn in src/lottery.rs, and the sysvar scan in src/instructions/resolve_pool.rs.
//
// These cover the on-chain machinery: open pins the miner quote, the per-request fee accrues to
// treasury, validator dedup, pair-mismatch, window timing, guards, single/multi-requester resolve.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    allways_swap_manager::constants::{FINALIZE_WINDOW_SECS, POOL_WINDOW_SECS, RESERVATION_FEE_LAMPORTS},
    allways_swap_manager::state::{MinerState, Pool, Reservation, Treasury},
    litesvm::LiteSVM,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_slot_hashes::SlotHashes,
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
const RATE: u128 = 1_500_000_000_000_000_000; // 1.5 × RATE_PRECISION (1e18)

fn pid() -> Pubkey {
    allways_swap_manager::id()
}
fn config_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"config"], &pid()).0
}
fn collateral_vault_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"collateral", m.as_ref()], &pid()).0
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

/// LiteSVM never advances Clock::slot on its own; the draw needs it to move.
fn set_slot(svm: &mut LiteSVM, slot: u64) {
    let mut clock = svm.get_sysvar::<Clock>();
    clock.slot = slot;
    svm.set_sysvar::<Clock>(&clock);
}

/// Populate SlotHashes with `slots` (any order — SlotHashes::new sorts descending, as on-chain).
/// Each slot's hash is `[slot as u8; 32]` so a test can tell which entry the draw consumed.
fn set_slot_hashes(svm: &mut LiteSVM, slots: &[u64]) {
    let entries: Vec<(u64, Hash)> = slots.iter().map(|&s| (s, Hash::new_from_array([s as u8; 32]))).collect();
    svm.set_sysvar::<SlotHashes>(&SlotHashes::new(&entries));
}

/// Crank #1 arms the draw on a future slot; make that slot (and a couple after it) exist.
/// Returns the armed seed slot.
fn arm_draw(svm: &mut LiteSVM, val: &Keypair, miner: &Pubkey) -> u64 {
    send(svm, resolve_ix(&val.pubkey(), miner), &val.pubkey(), val).expect("arm draw");
    let seed_slot = pool(svm, miner).seed_slot;
    assert_ne!(seed_slot, 0, "first resolve after close must arm a seed slot");
    seed_slot
}

/// Arm, produce the seed slot, then resolve. The normal two-crank path.
fn arm_and_resolve(svm: &mut LiteSVM, val: &Keypair, miner: &Pubkey) -> Result<(), String> {
    let seed_slot = arm_draw(svm, val, miner);
    set_slot_hashes(svm, &[seed_slot - 1, seed_slot, seed_slot + 1]);
    send(svm, resolve_ix(&val.pubkey(), miner), &val.pubkey(), val)
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
            collateral_vault: collateral_vault_pda(miner),
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
fn set_quote_ix(miner: &Pubkey, f: &str, t: &str, mfrom: &str, mto: &str, rate: u128) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetQuote {
            from_chain: f.to_string(),
            to_chain: t.to_string(),
            miner_from_addr: mfrom.to_string(),
            miner_to_addr: mto.to_string(),
            rate,
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
/// A bid: only the router competes for the seat (no taker, no amounts — those go to finalize).
fn open_ix(router: &Pubkey, miner: &Pubkey, f: &str, t: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::OpenOrRequest {
            from_chain: f.to_string(),
            to_chain: t.to_string(),
        }
        .data(),
        allways_swap_manager::accounts::OpenOrRequest {
            router: *router,
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
/// The seat winner names the fill (taker + amounts), making the reservation live.
#[allow(clippy::too_many_arguments)]
fn finalize_ix(
    router: &Pubkey,
    miner: &Pubkey,
    user: &Pubkey,
    ufrom: &str,
    uto: &str,
    collateral_amount: u64,
    from_amount: u128,
    to_amount: u128,
) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::FinalizeReservation {
            user: *user,
            user_from_addr: ufrom.to_string(),
            user_to_addr: uto.to_string(),
            collateral_amount,
            from_amount,
            to_amount,
        }
        .data(),
        allways_swap_manager::accounts::FinalizeReservation {
            router: *router,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner),
        }
        .to_account_metas(None),
    )
}
/// BTC→SOL fill: from_chain != "sol", so the collateral bind requires `to_amount == collateral_amount`.
/// `from_amount` is the BTC leg (irrelevant to the bind, kept for realism).
fn finalize_btc_sol(
    svm: &mut LiteSVM,
    winner: &Keypair,
    miner: &Pubkey,
    user: &Pubkey,
    ufrom: &str,
    uto: &str,
    collateral_amount: u64,
    from_amount: u128,
) -> Result<(), String> {
    let ix = finalize_ix(&winner.pubkey(), miner, user, ufrom, uto, collateral_amount, from_amount, collateral_amount as u128);
    send(svm, ix, &winner.pubkey(), winner)
}
/// arm+resolve a single-bidder pool (the sole bidder is the winner) then finalize a BTC→SOL fill.
#[allow(clippy::too_many_arguments)]
fn resolve_and_fill(
    svm: &mut LiteSVM,
    winner: &Keypair,
    miner: &Pubkey,
    user: &Pubkey,
    ufrom: &str,
    uto: &str,
    collateral_amount: u64,
    from_amount: u128,
) {
    arm_and_resolve(svm, winner, miner).expect("resolve");
    finalize_btc_sol(svm, winner, miner, user, ufrom, uto, collateral_amount, from_amount).expect("finalize");
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
    let _user = Keypair::new().pubkey();
    let t0 = treasury(&svm);

    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");

    let p = pool(&svm, &miner.pubkey());
    assert_eq!(p.from_chain, "BTC");
    assert_eq!(p.to_chain, "SOL");
    assert_eq!(p.miner_from_addr, MFROM, "pinned from MinerQuote");
    assert_eq!(p.miner_to_addr, MTO);
    assert_eq!(p.rate, RATE);
    assert_eq!(p.opened_at, BASE_TS);
    assert_eq!(p.closes_at, BASE_TS + POOL_WINDOW_SECS);
    assert_eq!(p.requests.len(), 1);
    assert_eq!(p.requests[0].router, vals[0].pubkey());
    assert_eq!(treasury(&svm), t0 + RESERVATION_FEE_LAMPORTS, "flat fee accrued to treasury");
}

#[test]
fn test_each_request_charges_fee() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let _user = Keypair::new().pubkey();
    let t0 = treasury(&svm);
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[1].pubkey(), &vals[1]).expect("join");
    assert_eq!(treasury(&svm), t0 + 2 * RESERVATION_FEE_LAMPORTS, "both opener and joiner pay");
    assert_eq!(pool(&svm, &miner.pubkey()).requests.len(), 2);
}

#[test]
fn test_repeat_bid_is_idempotent() {
    // A bid carries only the router — no taker, no amounts. A repeat call from the same router while
    // the pool is open is an idempotent no-op (no AlreadyRequested reject, no duplicate entry).
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("repeat");

    let p = pool(&svm, &miner.pubkey());
    assert_eq!(p.requests.len(), 1, "repeat bid does not duplicate the router's seat");
    assert_eq!(p.requests[0].router, vals[0].pubkey());
}

#[test]
fn test_pair_mismatch_rejected() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    // Miner also quotes BTC→TAO so that quote account exists for the join attempt.
    send(&mut svm, set_quote_ix(&miner.pubkey(), "BTC", "TAO", MFROM, "minerTAOaddr", 2_000_000_000_000_000_000), &miner.pubkey(), &miner).expect("quote2");
    let _user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open BTC/SOL");
    let mism = send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "TAO"), &vals[1].pubkey(), &vals[1]);
    assert!(mism.is_err(), "joining with a different pair than the pinned one must be rejected");
}

#[test]
fn test_single_requester_resolve_then_finalize_creates_reservation() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");

    // before close → cannot resolve
    let early = send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]);
    assert!(early.is_err(), "resolve before window close must fail");

    // warp past close, resolve (sole entrant wins regardless of seed) → UNFILLED reservation
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    arm_and_resolve(&mut svm, &vals[0], &miner.pubkey()).expect("resolve");

    // draw pins router + miner quote, but the reservation is unfilled until the winner finalizes
    let r = reservation(&svm, &miner.pubkey());
    assert_eq!(r.reserved_until, 0, "reservation is UNFILLED right after the draw");
    assert_eq!(r.router, vals[0].pubkey(), "seat winner pinned");
    assert_eq!(r.finalize_by, BASE_TS + POOL_WINDOW_SECS + 1 + FINALIZE_WINDOW_SECS, "finalize deadline set");
    assert_eq!(r.from_chain, "BTC");
    assert_eq!(r.to_chain, "SOL");
    assert_eq!(r.miner_from_addr, MFROM, "pinned miner quote carried into reservation");
    assert_eq!(r.miner_to_addr, MTO);
    assert_eq!(r.rate, RATE);
    // pool reset for reuse the moment the draw resolves
    assert_eq!(pool(&svm, &miner.pubkey()).opened_at, 0, "pool reset after resolve");
    assert!(pool(&svm, &miner.pubkey()).requests.is_empty());

    // the seat winner fills it (BTC→SOL: collateral_amount == to_amount)
    finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "userBTC", "userSOL", 2_000_000_000, 100_000).expect("finalize");

    let r = reservation(&svm, &miner.pubkey());
    assert_eq!(r.reserved_until, BASE_TS + POOL_WINDOW_SECS + 1 + TTL, "reserved with TTL from finalize time");
    assert_eq!(r.user, user, "taker pinned at finalize");
    assert_eq!(r.from_addr, "userBTC", "winner's user source addr");
    assert_eq!(r.collateral_amount, 2_000_000_000);
    assert_eq!(r.created_at, BASE_TS + POOL_WINDOW_SECS + 1, "source-freshness floor = finalize time");
}

#[test]
fn test_multi_requester_resolve_picks_one() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let u0 = Keypair::new().pubkey();
    let u1 = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[1].pubkey(), &vals[1]).expect("join");

    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    arm_and_resolve(&mut svm, &vals[2], &miner.pubkey()).expect("resolve");

    // the draw pins the winning ROUTER (unfilled); the winner is one of the two bidders.
    let r = reservation(&svm, &miner.pubkey());
    assert_eq!(r.reserved_until, 0, "unfilled until the winner finalizes");
    assert!(r.router == vals[0].pubkey() || r.router == vals[1].pubkey(), "winner is one of the two bidders");
    let _ = (u0, u1);
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
    let _user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    let late = send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[1].pubkey(), &vals[1]);
    assert!(late.is_err(), "joining after the window closed must fail (must resolve first)");
}

#[test]
fn test_amount_bounds_fire_at_finalize() {
    // Swap-size bounds moved from open to finalize (the amount is only known at fill). Bid+draw
    // succeed; finalize with an out-of-bounds collateral_amount is rejected, in-bounds succeeds.
    let (mut svm, _admin, vals, miner) = setup(1_000_000_000, 5_000_000_000);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("bid");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    arm_and_resolve(&mut svm, &vals[0], &miner.pubkey()).expect("resolve");

    let below = finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 500_000_000, 1);
    assert!(below.is_err(), "below min rejected at finalize");
    let above = finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 9_000_000_000, 1);
    assert!(above.is_err(), "above max rejected at finalize");
    // in-bounds fills
    finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 2_000_000_000, 1).expect("in-bounds finalize");
    assert!(reservation(&svm, &miner.pubkey()).reserved_until > 0);
}

#[test]
fn test_requires_active_miner() {
    let (mut svm, _admin, vals, _miner) = setup(0, 0);
    // an inactive miner that posted collateral + a quote but was never activated
    let inactive = Keypair::new();
    svm.airdrop(&inactive.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, post_ix(&inactive.pubkey(), 1_000_000_000), &inactive.pubkey(), &inactive).expect("post");
    send(&mut svm, set_quote_ix(&inactive.pubkey(), "BTC", "SOL", MFROM, MTO, RATE), &inactive.pubkey(), &inactive).expect("quote");
    let _user = Keypair::new().pubkey();
    let res = send(&mut svm, open_ix(&vals[0].pubkey(), &inactive.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]);
    assert!(res.is_err(), "inactive miner cannot be pooled");
}

#[test]
fn test_open_blocked_while_reserved() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    resolve_and_fill(&mut svm, &vals[0], &miner.pubkey(), &user, "u1", "uSOL", 2_000_000_000, 1);
    assert!(reservation(&svm, &miner.pubkey()).reserved_until > 0);

    // a new open is blocked while the reservation is active (filled)
    let blocked = send(&mut svm, open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[1].pubkey(), &vals[1]);
    assert!(blocked.is_err(), "cannot open a new pool while a reservation is active");
}

#[test]
fn test_open_pool_blocks_deactivate() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let _user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    assert_ne!(pool(&svm, &miner.pubkey()).opened_at, 0);

    // miner is busy the moment a pool opens (pre-resolve) — cannot self-deactivate
    let blocked = send(&mut svm, deactivate_ix(&miner.pubkey()), &miner.pubkey(), &miner);
    assert!(blocked.is_err(), "cannot self-deactivate while a pool is open");
}

#[test]
fn test_reservation_blocks_deactivate_until_expiry() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    resolve_and_fill(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 2_000_000_000, 1);
    assert!(is_active(&svm, &miner.pubkey()));

    let blocked = send(&mut svm, deactivate_ix(&miner.pubkey()), &miner.pubkey(), &miner);
    assert!(blocked.is_err(), "cannot self-deactivate while reserved");

    // deactivate gates on busy_until (set conservatively at bid to cover the whole window); warp past it.
    let busy = busy_until(&svm, &miner.pubkey());
    set_clock(&mut svm, busy + 1);
    send(&mut svm, deactivate_ix(&miner.pubkey()), &miner.pubkey(), &miner).expect("deactivate after busy lock lifts");
    assert!(!is_active(&svm, &miner.pubkey()), "miner deactivated once the busy lock lifted");
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
fn test_finalize_requires_overcollateralization() {
    // setup posts 10 SOL collateral; the requirement is 1.10×. The gate moved to finalize (the amount
    // is only known there): a fill whose 1.10× need exceeds collateral is rejected so vote_initiate
    // can't later strand a user.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let user = Keypair::new().pubkey();
    assert_eq!(collateral(&svm, &miner.pubkey()), 10_000_000_000);
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("bid");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    arm_and_resolve(&mut svm, &vals[0], &miner.pubkey()).expect("resolve");

    // 9.5 SOL × 1.10 = 10.45 SOL > 10 SOL → rejected.
    let too_big = finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 9_500_000_000, 1);
    assert!(too_big.is_err(), "fill needing >collateral at 1.10× must be rejected at finalize");

    // 9 SOL × 1.10 = 9.9 SOL ≤ 10 SOL → accepted.
    finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 9_000_000_000, 1).expect("affordable fill");
}

#[test]
fn test_cannot_force_deactivate_while_pooled() {
    // A miner with an open pool is busy and must not be force-deactivatable mid-window — deactivation
    // is only for idle miners (preserves the busy ⟹ active invariant; user req / review #3).
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let _user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    let blocked = send(&mut svm, vote_deactivate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]);
    assert!(blocked.is_err(), "cannot force-deactivate a miner with an open pool");
    assert!(is_active(&svm, &miner.pubkey()), "miner stays active");
}

#[test]
fn test_cannot_force_deactivate_while_reserved() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let _user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    arm_and_resolve(&mut svm, &vals[0], &miner.pubkey()).expect("resolve");
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
// NOTE: the old `test_update_reflected_in_reservation` was removed — under two-phase a bid carries no
// amounts, so there is no bid content to "reflect". The winner names amounts at finalize instead
// (covered by test_single_requester_resolve_then_finalize_creates_reservation).

#[test]
fn test_repeat_bid_is_free() {
    // First bid pays the reservation fee; a same-router repeat bid in the same window is free.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let t0 = treasury(&svm);
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    assert_eq!(treasury(&svm), t0 + RESERVATION_FEE_LAMPORTS, "first entry pays the fee");
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("repeat");
    assert_eq!(treasury(&svm), t0 + RESERVATION_FEE_LAMPORTS, "in-window repeat bid is free (no extra fee)");
}

#[test]
fn test_bid_after_window_close_fails() {
    // Once the window closes, no further bids (must resolve first).
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    let late = send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"), &vals[0].pubkey(), &vals[0]);
    assert!(late.is_err(), "cannot bid after the window closed");
}

// --- draw entropy: arm-after-close, skip tolerance, no predictable fallback ---

/// Open a pool with one bid and warp past the window close.
fn open_and_close(svm: &mut LiteSVM, vals: &[Keypair], miner: &Keypair) {
    send(
        svm,
        open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("open");
    set_clock(svm, BASE_TS + POOL_WINDOW_SECS + 1);
}

#[test]
fn test_open_does_not_pin_seed_slot() {
    // The entropy must not be knowable while bids are still being placed: pinning at open let a
    // late joiner read the slot hash and enter only when it would win.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let _user = Keypair::new().pubkey();
    send(
        &mut svm,
        open_ix(&vals[0].pubkey(), &miner.pubkey(), "BTC", "SOL"),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("open");
    assert_eq!(pool(&svm, &miner.pubkey()).seed_slot, 0, "seed slot must stay unpinned during the bidding window");
}

#[test]
fn test_arming_creates_no_reservation() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    open_and_close(&mut svm, &vals, &miner);
    let seed_slot = arm_draw(&mut svm, &vals[0], &miner.pubkey());
    assert!(seed_slot > 0);
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, 0, "arming must not draw a winner");
    assert_ne!(pool(&svm, &miner.pubkey()).opened_at, 0, "pool stays open until the draw resolves");
}

#[test]
fn test_resolve_before_seed_slot_is_produced_errors() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    open_and_close(&mut svm, &vals, &miner);
    let seed_slot = arm_draw(&mut svm, &vals[0], &miner.pubkey());

    // Chain has not reached the armed slot yet.
    set_slot_hashes(&mut svm, &[seed_slot - 2, seed_slot - 1]);
    let e = send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0])
        .expect_err("must not resolve before the seed slot exists");
    assert!(e.contains("SeedSlotNotYetProduced"), "unexpected error: {e}");
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, 0, "no reservation from an unproduced seed");
    assert_eq!(pool(&svm, &miner.pubkey()).seed_slot, seed_slot, "armed slot must not drift on a failed retry");
}

#[test]
fn test_skipped_seed_slot_resolves_from_next_produced_slot() {
    // A skipped seed slot used to fall through to a seed derivable at pool-open. Now it takes the
    // lowest produced slot above it and draws normally.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    open_and_close(&mut svm, &vals, &miner);
    let seed_slot = arm_draw(&mut svm, &vals[0], &miner.pubkey());

    // Buffer straddles the seed slot, but the seed slot itself was never produced (leader skipped it).
    set_slot_hashes(&mut svm, &[seed_slot - 1, seed_slot + 1, seed_slot + 2]);
    send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0])
        .expect("skipped seed slot must still resolve");
    // the draw resolved (unfilled): a winning router is pinned and the pool reset.
    assert_eq!(reservation(&svm, &miner.pubkey()).router, vals[0].pubkey(), "winner drawn");
    assert_eq!(pool(&svm, &miner.pubkey()).seed_slot, 0, "pool reset re-arms next contest");
}

#[test]
fn test_rolled_off_seed_slot_rearms_instead_of_drawing() {
    // After a long stall the armed slot ages out of SlotHashes. Drawing from whatever is left would
    // let the caller pick the hash by choosing when to crank, so we re-arm.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    open_and_close(&mut svm, &vals, &miner);
    let first = arm_draw(&mut svm, &vals[0], &miner.pubkey());

    // >512 slots later: every retained slot is newer than the armed one.
    set_slot(&mut svm, first + 601);
    set_slot_hashes(&mut svm, &[first + 600, first + 601]);
    send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("re-arm");

    let second = pool(&svm, &miner.pubkey()).seed_slot;
    assert_ne!(second, first, "must re-arm on a fresh slot");
    assert_ne!(second, 0);
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, 0, "no draw from a rolled-off window");
    assert_ne!(pool(&svm, &miner.pubkey()).opened_at, 0, "bids survive the re-arm");
}

#[test]
fn test_armed_slot_is_ahead_of_every_produced_slot() {
    // The security property: at arm time the seed slot does not exist, so nobody — not the arming
    // cranker, not a bidder — can know the hash the draw will consume.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    set_slot(&mut svm, 1_000);
    set_slot_hashes(&mut svm, &[998, 999, 1_000]);
    open_and_close(&mut svm, &vals, &miner);

    let seed_slot = arm_draw(&mut svm, &vals[0], &miner.pubkey());
    assert!(seed_slot > 1_000, "armed slot {seed_slot} must exceed the newest produced slot (1000)");
}

// ============================ two-phase finalize / reap coverage ============================

use solana_keccak_hasher::hashv;

fn swap_pda(key: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"swap", key], &pid()).0
}
fn swap_key_of(from_tx_hash: &str) -> [u8; 32] {
    hashv(&[from_tx_hash.as_bytes()]).to_bytes()
}
fn submit_claim_ix(caller: &Pubkey, miner: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key_of(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SubmitSwapClaim {
            swap_key: key,
            from_tx_hash: from_tx_hash.to_string(),
            from_tx_block: 1,
        }
        .data(),
        allways_swap_manager::accounts::SubmitSwapClaim {
            caller: *caller,
            config: config_pda(),
            miner: *miner,
            reservation: resv_pda(miner),
            swap: swap_pda(&key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn close_unfilled_ix(caller: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::CloseUnfilledReservation {}.data(),
        allways_swap_manager::accounts::CloseUnfilledReservation {
            caller: *caller,
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner),
        }
        .to_account_metas(None),
    )
}

/// Bid + arm/resolve a single-bidder pool, leaving an UNFILLED reservation whose router is `vals[0]`.
/// Clock left at BASE_TS + POOL_WINDOW_SECS + 1.
fn bid_and_draw(svm: &mut LiteSVM, vals: &[Keypair], miner: &Pubkey) {
    send(svm, open_ix(&vals[0].pubkey(), miner, "BTC", "SOL"), &vals[0].pubkey(), &vals[0]).expect("bid");
    set_clock(svm, BASE_TS + POOL_WINDOW_SECS + 1);
    arm_and_resolve(svm, &vals[0], miner).expect("resolve");
}

#[test]
fn test_finalize_rejects_non_router_signer() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    bid_and_draw(&mut svm, &vals, &miner.pubkey());
    let user = Keypair::new().pubkey();
    // vals[1] did not win the seat — it must not be able to fill vals[0]'s reservation.
    let ix = finalize_ix(&vals[1].pubkey(), &miner.pubkey(), &user, "u", "uSOL", 2_000_000_000, 1, 2_000_000_000);
    let e = send(&mut svm, ix, &vals[1].pubkey(), &vals[1]);
    assert!(e.is_err(), "only reservation.router may finalize");
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, 0, "still unfilled");
}

#[test]
fn test_open_blocked_during_pending_finalize_window() {
    // Regression: a drawn-but-UNFILLED reservation (reserved_until == 0, finalize_by in the future)
    // holds the miner through its finalize window. A fresh bid must be rejected (MinerReserved) so a
    // new draw can't overwrite/evict the seat winner before they get to finalize.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    bid_and_draw(&mut svm, &vals, &miner.pubkey());
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, 0, "unfilled");
    assert_ne!(reservation(&svm, &miner.pubkey()).finalize_by, 0, "finalize window armed");
    let e = send(
        &mut svm,
        open_ix(&vals[1].pubkey(), &miner.pubkey(), "BTC", "SOL"),
        &vals[1].pubkey(),
        &vals[1],
    );
    assert!(e.is_err(), "a fresh contest must not evict the pending seat winner");
}

#[test]
fn test_finalize_rejects_second_fill() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    bid_and_draw(&mut svm, &vals, &miner.pubkey());
    let user = Keypair::new().pubkey();
    finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 2_000_000_000, 1).expect("first fill");
    let again = finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 2_000_000_000, 1);
    assert!(again.is_err(), "a filled reservation cannot be filled again (AlreadyFilled)");
}

#[test]
fn test_finalize_rejects_after_window() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    bid_and_draw(&mut svm, &vals, &miner.pubkey());
    let finalize_by = reservation(&svm, &miner.pubkey()).finalize_by;
    set_clock(&mut svm, finalize_by + 1);
    let user = Keypair::new().pubkey();
    let late = finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 2_000_000_000, 1);
    assert!(late.is_err(), "finalize past finalize_by is rejected (FinalizeWindowExpired)");
}

#[test]
fn test_collateral_bind_rejects_mismatch_spoke_to_sol() {
    // BTC→SOL: SOL is the destination leg, so collateral_amount must == to_amount.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    bid_and_draw(&mut svm, &vals, &miner.pubkey());
    let user = Keypair::new().pubkey();
    // collateral_amount=2e9 but to_amount=1 (mismatch) → rejected.
    let bad = finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &user, "u", "uSOL", 2_000_000_000, 5, 1);
    assert!(send(&mut svm, bad, &vals[0].pubkey(), &vals[0]).is_err(), "collateral_amount must equal the SOL (to) leg");
    // matching triple succeeds.
    let ok = finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &user, "u", "uSOL", 2_000_000_000, 5, 2_000_000_000);
    send(&mut svm, ok, &vals[0].pubkey(), &vals[0]).expect("matching bind fills");
}

#[test]
fn test_collateral_bind_rejects_mismatch_sol_to_spoke() {
    // sol→btc: sol is the SOURCE (numeraire) leg, so collateral_amount must == from_amount.
    // NB: the bind is case-sensitive on NUMERAIRE_CHAIN == "sol" (lowercase, as the real system uses);
    // this test uses lowercase chain ids so `from_chain == NUMERAIRE_CHAIN` holds.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    send(&mut svm, set_quote_ix(&miner.pubkey(), "sol", "btc", MTO, MFROM, RATE), &miner.pubkey(), &miner).expect("sol->btc quote");
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), "sol", "btc"), &vals[0].pubkey(), &vals[0]).expect("bid");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    arm_and_resolve(&mut svm, &vals[0], &miner.pubkey()).expect("resolve");
    let user = Keypair::new().pubkey();
    // collateral_amount=2e9 but from_amount=1 (mismatch) → rejected.
    let bad = finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &user, "uSOL", "uBTC", 2_000_000_000, 1, 9);
    assert!(send(&mut svm, bad, &vals[0].pubkey(), &vals[0]).is_err(), "collateral_amount must equal the sol (from) leg");
    // matching triple succeeds.
    let ok = finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &user, "uSOL", "uBTC", 2_000_000_000, 2_000_000_000, 9);
    send(&mut svm, ok, &vals[0].pubkey(), &vals[0]).expect("matching bind fills");
}

#[test]
fn test_submit_claim_rejects_unfilled_reservation() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    bid_and_draw(&mut svm, &vals, &miner.pubkey());
    // reservation is unfilled (reserved_until == 0) → claim must be rejected (NoReservation).
    let e = send(&mut svm, submit_claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx"), &vals[0].pubkey(), &vals[0]);
    assert!(e.is_err(), "cannot claim against an unfilled reservation");
}

#[test]
fn test_close_unfilled_frees_the_miner() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    bid_and_draw(&mut svm, &vals, &miner.pubkey());
    let finalize_by = reservation(&svm, &miner.pubkey()).finalize_by;

    // before the deadline it is NOT reapable.
    let early = send(&mut svm, close_unfilled_ix(&vals[2].pubkey(), &miner.pubkey()), &vals[2].pubkey(), &vals[2]);
    assert!(early.is_err(), "cannot reap before finalize_by");

    // after the deadline anyone may reap it, freeing busy_until.
    set_clock(&mut svm, finalize_by + 1);
    send(&mut svm, close_unfilled_ix(&vals[2].pubkey(), &miner.pubkey()), &vals[2].pubkey(), &vals[2]).expect("reap");
    assert_eq!(busy_until(&svm, &miner.pubkey()), finalize_by + 1, "busy_until freed to now");
    assert_eq!(reservation(&svm, &miner.pubkey()).finalize_by, 0, "finalize_by cleared");
}

#[test]
fn test_close_unfilled_rejected_while_filled() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    bid_and_draw(&mut svm, &vals, &miner.pubkey());
    let user = Keypair::new().pubkey();
    finalize_btc_sol(&mut svm, &vals[0], &miner.pubkey(), &user, "u", "uSOL", 2_000_000_000, 1).expect("fill");
    // a FILLED reservation must never be reapable — that would free a miner mid-swap.
    let finalize_by = reservation(&svm, &miner.pubkey()).finalize_by;
    set_clock(&mut svm, finalize_by + 1_000_000);
    let e = send(&mut svm, close_unfilled_ix(&vals[2].pubkey(), &miner.pubkey()), &vals[2].pubkey(), &vals[2]);
    assert!(e.is_err(), "a filled reservation is not reapable (NotReapable)");
}

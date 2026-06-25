// Phase 11 — deadline extensions: extend_reservation / extend_timeout (LiteSVM, clock-controlled).
//   cargo test -p allways_swap_manager --test test_extensions
//
// Single-validator, no-quorum deadline pushes bounded by the per-account `max_extend_at` ceiling
// frozen at creation. Covers: happy-path push (reservation + swap, Active and Fulfilled), the ceiling
// cap, the monotonic guard, and the validator-only gate.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    allways_swap_manager::constants::{MAX_TOTAL_EXTENSION_SECS, POOL_WINDOW_SECS},
    allways_swap_manager::state::{MinerState, Reservation, Swap},
    litesvm::LiteSVM,
    solana_keccak_hasher::hashv,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;
const SLOT_HASHES_ID: Pubkey = Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111");
const REQ_ACTIVATE: u8 = 0;
const REQ_INITIATE: u8 = 2;
const BASE_TS: i64 = 1_700_000_000;
const TTL: i64 = 1_800;
const TIMEOUT_SECS: i64 = 3_600;
const COLLATERAL: u64 = 10_000_000_000;
const SOL_AMOUNT: u64 = 2_000_000_000;

const FROM_ADDR: &str = "userBTCaddr";
const FROM_CHAIN: &str = "BTC";
const TO_CHAIN: &str = "SOL";
const MINER_FROM: &str = "minerBTCaddr";
const MINER_TO: &str = "minerSOLaddr";
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
fn vote_pda(req: u8, key: &[u8]) -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[req], key], &pid()).0
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
fn swap_pda(key: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"swap", key], &pid()).0
}
fn tx_pda(key: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"tx", key], &pid()).0
}
fn swap_key(from_tx_hash: &str) -> [u8; 32] {
    hashv(&[from_tx_hash.as_bytes()]).to_bytes()
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

fn init_ix(admin: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Initialize {
            min_collateral: 1_000_000_000,
            max_collateral: 0,
            fulfillment_timeout_secs: TIMEOUT_SECS,
            consensus_threshold_percent: 66,
            min_swap_amount: 0,
            max_swap_amount: 0,
            reservation_ttl_secs: TTL,
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
fn add_validator_ix(admin: &Pubkey, v: Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::AddValidator { validator: v, weight: 1 }.data(),
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
            vote_round: vote_pda(REQ_ACTIVATE, miner.as_ref()),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn set_quote_ix(miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetQuote {
            from_chain: FROM_CHAIN.to_string(),
            to_chain: TO_CHAIN.to_string(),
            miner_from_addr: MINER_FROM.to_string(),
            miner_to_addr: MINER_TO.to_string(),
            rate: RATE,
            liquidity: 1_000,
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *miner,
            quote: quote_pda(miner, FROM_CHAIN, TO_CHAIN),
            treasury: treasury_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn open_ix(validator: &Pubkey, miner: &Pubkey, user: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::OpenOrRequest {
            from_chain: FROM_CHAIN.to_string(),
            to_chain: TO_CHAIN.to_string(),
            user: *user,
            user_from_addr: FROM_ADDR.to_string(),
            user_to_addr: "userSOLaddr".to_string(),
            sol_amount: SOL_AMOUNT,
            from_amount: 100_000,
            to_amount: 0,
        }
        .data(),
        allways_swap_manager::accounts::OpenOrRequest {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            quote: quote_pda(miner, FROM_CHAIN, TO_CHAIN),
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
fn initiate_ix(validator: &Pubkey, miner: &Pubkey, from_tx_hash: &str, user: &Pubkey) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteInitiate {
            swap_key: key,
            from_tx_hash: from_tx_hash.to_string(),
            from_tx_block: 800_000,
            user: *user,
            user_from_address: FROM_ADDR.to_string(),
            user_to_address: "userSOLaddr".to_string(),
        }
        .data(),
        allways_swap_manager::accounts::VoteInitiate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner),
            vote_round: vote_pda(REQ_INITIATE, miner.as_ref()),
            tx_marker: tx_pda(&key),
            swap: swap_pda(&key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn fulfill_ix(miner: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::MarkFulfilled {
            swap_key: key,
            to_tx_hash: "destTxHash".to_string(),
            to_tx_block: 200,
        }
        .data(),
        allways_swap_manager::accounts::MarkFulfilled { miner: *miner, swap: swap_pda(&key) }
            .to_account_metas(None),
    )
}
fn extend_reservation_ix(validator: &Pubkey, miner: &Pubkey, target_at: i64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::ExtendReservation { target_at }.data(),
        allways_swap_manager::accounts::ExtendReservation {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner),
        }
        .to_account_metas(None),
    )
}
fn extend_timeout_ix(validator: &Pubkey, miner: &Pubkey, from_tx_hash: &str, target_at: i64) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::ExtendTimeout { swap_key: key, target_at }.data(),
        allways_swap_manager::accounts::ExtendTimeout {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            swap: swap_pda(&key),
        }
        .to_account_metas(None),
    )
}

fn reservation(svm: &LiteSVM, m: &Pubkey) -> Reservation {
    let a = svm.get_account(&resv_pda(m)).unwrap();
    Reservation::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn swap(svm: &LiteSVM, from_tx_hash: &str) -> Swap {
    let a = svm.get_account(&swap_pda(&swap_key(from_tx_hash))).unwrap();
    Swap::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn busy_until(svm: &LiteSVM, m: &Pubkey) -> i64 {
    let a = svm.get_account(&miner_pda(m)).unwrap();
    MinerState::try_deserialize(&mut a.data.as_slice()).unwrap().busy_until
}

/// init + 3 validators + active miner + a confirmed reservation, clock at BASE_TS. The reservation
/// is created at BASE_TS-100+window so it's live at BASE_TS. Returns (svm, vals, miner).
fn setup() -> (LiteSVM, Vec<Keypair>, Keypair) {
    let mut svm = LiteSVM::new();
    svm.add_program(pid(), include_bytes!("../../../target/deploy/allways_swap_manager.so")).unwrap();
    set_clock(&mut svm, BASE_TS);

    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, init_ix(&admin.pubkey()), &admin.pubkey(), &admin).expect("init");

    let mut vals = Vec::new();
    for _ in 0..3 {
        let v = Keypair::new();
        svm.airdrop(&v.pubkey(), 100_000_000_000).unwrap();
        send(&mut svm, add_validator_ix(&admin.pubkey(), v.pubkey()), &admin.pubkey(), &admin).expect("add val");
        vals.push(v);
    }

    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, post_ix(&miner.pubkey(), COLLATERAL), &miner.pubkey(), &miner).expect("post");
    send(&mut svm, set_quote_ix(&miner.pubkey()), &miner.pubkey(), &miner).expect("quote");
    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("a0");
    send(&mut svm, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("a1");

    let setup_ts = BASE_TS - 100;
    set_clock(&mut svm, setup_ts);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), &user), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, setup_ts + POOL_WINDOW_SECS + 1);
    send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("resolve");
    set_clock(&mut svm, BASE_TS);
    (svm, vals, miner)
}

fn do_initiate(svm: &mut LiteSVM, vals: &[Keypair], miner: &Pubkey, tx: &str, user: &Pubkey) {
    send(svm, initiate_ix(&vals[0].pubkey(), miner, tx, user), &vals[0].pubkey(), &vals[0]).expect("i0");
    send(svm, initiate_ix(&vals[1].pubkey(), miner, tx, user), &vals[1].pubkey(), &vals[1]).expect("i1");
}

// ---- extend_reservation ----

#[test]
fn test_extend_reservation_pushes_deadline_and_busy_lock() {
    let (mut svm, vals, miner) = setup();
    let r = reservation(&svm, &miner.pubkey());
    assert_eq!(r.max_extend_at, r.reserved_until + MAX_TOTAL_EXTENSION_SECS, "ceiling frozen at creation");

    let target = r.reserved_until + 300;
    send(&mut svm, extend_reservation_ix(&vals[0].pubkey(), &miner.pubkey(), target), &vals[0].pubkey(), &vals[0]).expect("extend");

    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, target, "deadline pushed");
    assert_eq!(busy_until(&svm, &miner.pubkey()), target, "busy lock kept in sync");
}

#[test]
fn test_extend_reservation_repeated_until_ceiling() {
    let (mut svm, vals, miner) = setup();
    let ceiling = reservation(&svm, &miner.pubkey()).max_extend_at;

    // Two successive nudges, both under the ceiling, accumulate.
    let t1 = reservation(&svm, &miner.pubkey()).reserved_until + 1_000;
    send(&mut svm, extend_reservation_ix(&vals[0].pubkey(), &miner.pubkey(), t1), &vals[0].pubkey(), &vals[0]).expect("nudge1");
    let t2 = ceiling; // land exactly on the ceiling
    send(&mut svm, extend_reservation_ix(&vals[1].pubkey(), &miner.pubkey(), t2), &vals[1].pubkey(), &vals[1]).expect("nudge2");
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, ceiling);

    // One past the ceiling is rejected.
    let over = send(&mut svm, extend_reservation_ix(&vals[0].pubkey(), &miner.pubkey(), ceiling + 1), &vals[0].pubkey(), &vals[0]);
    assert!(over.is_err(), "cannot extend past the frozen ceiling");
}

#[test]
fn test_extend_reservation_must_be_monotonic() {
    let (mut svm, vals, miner) = setup();
    let current = reservation(&svm, &miner.pubkey()).reserved_until;
    let backward = send(&mut svm, extend_reservation_ix(&vals[0].pubkey(), &miner.pubkey(), current), &vals[0].pubkey(), &vals[0]);
    assert!(backward.is_err(), "target not later than current deadline is rejected");
}

#[test]
fn test_extend_reservation_validator_only() {
    let (mut svm, _vals, miner) = setup();
    let outsider = Keypair::new();
    svm.airdrop(&outsider.pubkey(), 1_000_000_000).unwrap();
    let target = reservation(&svm, &miner.pubkey()).reserved_until + 300;
    let res = send(&mut svm, extend_reservation_ix(&outsider.pubkey(), &miner.pubkey(), target), &outsider.pubkey(), &outsider);
    assert!(res.is_err(), "non-validator cannot extend");
}

// ---- extend_timeout ----

#[test]
fn test_extend_timeout_active_pushes_deadline_and_busy_lock() {
    let (mut svm, vals, miner) = setup();
    let user = Keypair::new().pubkey();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1", &user);

    let s = swap(&svm, "srctx1");
    assert_eq!(s.max_extend_at, s.timeout_at + MAX_TOTAL_EXTENSION_SECS, "ceiling frozen at creation");

    let target = s.timeout_at + 300;
    send(&mut svm, extend_timeout_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1", target), &vals[0].pubkey(), &vals[0]).expect("extend");

    assert_eq!(swap(&svm, "srctx1").timeout_at, target, "timeout pushed while Active");
    assert_eq!(busy_until(&svm, &miner.pubkey()), target, "busy lock kept in sync");
}

#[test]
fn test_extend_timeout_after_fulfilled() {
    let (mut svm, vals, miner) = setup();
    let user = Keypair::new().pubkey();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1", &user);
    send(&mut svm, fulfill_ix(&miner.pubkey(), "srctx1"), &miner.pubkey(), &miner).expect("fulfill");

    let target = swap(&svm, "srctx1").timeout_at + 300;
    send(&mut svm, extend_timeout_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1", target), &vals[0].pubkey(), &vals[0]).expect("extend after fulfill");
    assert_eq!(swap(&svm, "srctx1").timeout_at, target, "timeout pushed while Fulfilled");
}

#[test]
fn test_extend_timeout_ceiling_and_monotonic() {
    let (mut svm, vals, miner) = setup();
    let user = Keypair::new().pubkey();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1", &user);
    let s = swap(&svm, "srctx1");

    let over = send(&mut svm, extend_timeout_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1", s.max_extend_at + 1), &vals[0].pubkey(), &vals[0]);
    assert!(over.is_err(), "cannot extend past the frozen ceiling");
    let backward = send(&mut svm, extend_timeout_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1", s.timeout_at), &vals[0].pubkey(), &vals[0]);
    assert!(backward.is_err(), "target not later than current timeout is rejected");
}

#[test]
fn test_extend_timeout_validator_only() {
    let (mut svm, vals, miner) = setup();
    let user = Keypair::new().pubkey();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1", &user);
    let outsider = Keypair::new();
    svm.airdrop(&outsider.pubkey(), 1_000_000_000).unwrap();
    let target = swap(&svm, "srctx1").timeout_at + 300;
    let res = send(&mut svm, extend_timeout_ix(&outsider.pubkey(), &miner.pubkey(), "srctx1", target), &outsider.pubkey(), &outsider);
    assert!(res.is_err(), "non-validator cannot extend");
}

// Phase 4 — swap lifecycle: initiate / fulfill / confirm / timeout (LiteSVM, clock-controlled).
//   cargo test -p allways_swap_manager --test test_swap
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    allways_swap_manager::constants::{POOL_WINDOW_SECS, RESERVATION_FEE_LAMPORTS},
    allways_swap_manager::state::{MinerState, Swap, Treasury},
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
const REQ_CONFIRM: u8 = 6;
const REQ_TIMEOUT: u8 = 7;
const BASE_TS: i64 = 1_700_000_000;
const TTL: i64 = 1_800;
const TIMEOUT_SECS: i64 = 3_600;
const COLLATERAL: u64 = 10_000_000_000; // 10 SOL
const SOL_AMOUNT: u64 = 2_000_000_000; // 2 SOL swap size

// reservation quote (must be consistent reserve→initiate)
const FROM_ADDR: &str = "userBTCaddr";
const FROM_CHAIN: &str = "BTC";
const TO_CHAIN: &str = "SOL";
const MINER_FROM: &str = "minerBTCaddr";
const MINER_TO: &str = "minerSOLaddr";
const RATE: &str = "1.5";

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
            min_collateral: 1_000_000_000, // 1 SOL
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
            rate: RATE.to_string(),
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
#[allow(clippy::too_many_arguments)]
fn initiate_ix(
    validator: &Pubkey,
    miner: &Pubkey,
    from_tx_hash: &str,
    user: &Pubkey,
    user_to: &str,
) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteInitiate {
            swap_key: key,
            from_tx_hash: from_tx_hash.to_string(),
            from_tx_block: 800_000,
            user: *user,
            user_from_address: FROM_ADDR.to_string(),
            user_to_address: user_to.to_string(),
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
fn confirm_ix(validator: &Pubkey, miner: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::ConfirmSwap { swap_key: key }.data(),
        allways_swap_manager::accounts::ConfirmSwap {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            collateral_vault: collateral_vault_pda(miner),
            treasury: treasury_pda(),
            swap: swap_pda(&key),
            vote_round: vote_pda(REQ_CONFIRM, &key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn timeout_ix(validator: &Pubkey, miner: &Pubkey, user: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::TimeoutSwap { swap_key: key }.data(),
        allways_swap_manager::accounts::TimeoutSwap {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            collateral_vault: collateral_vault_pda(miner),
            user: *user,
            swap: swap_pda(&key),
            vote_round: vote_pda(REQ_TIMEOUT, &key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

fn miner_state(svm: &LiteSVM, m: &Pubkey) -> MinerState {
    let a = svm.get_account(&miner_pda(m)).unwrap();
    MinerState::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn treasury_total(svm: &LiteSVM) -> u64 {
    let a = svm.get_account(&treasury_pda()).unwrap();
    Treasury::try_deserialize(&mut a.data.as_slice()).unwrap().total
}
fn collateral_vault_lamports(svm: &LiteSVM, m: &Pubkey) -> u64 {
    svm.get_account(&collateral_vault_pda(m)).unwrap().lamports
}
fn lamports(svm: &LiteSVM, p: &Pubkey) -> u64 {
    svm.get_account(p).map(|a| a.lamports).unwrap_or(0)
}

/// init + 3 validators + active miner + a confirmed reservation. Returns (svm, vals, miner,
/// vault rent reserve). Clock at BASE_TS.
fn setup() -> (LiteSVM, Vec<Keypair>, Keypair, u64) {
    setup_with_collateral(COLLATERAL)
}

/// As `setup`, but the miner posts an arbitrary collateral amount. Must be ≥ 1.10× SOL_AMOUNT or the
/// in-setup `open` is rejected by the over-collateralization entry gate.
fn setup_with_collateral(collateral: u64) -> (LiteSVM, Vec<Keypair>, Keypair, u64) {
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
    send(&mut svm, post_ix(&miner.pubkey(), collateral), &miner.pubkey(), &miner).expect("post");
    // The per-miner collateral vault is created by the deposit above; derive its rent reserve.
    let rent_reserve = collateral_vault_lamports(&svm, &miner.pubkey()) - collateral;
    send(&mut svm, set_quote_ix(&miner.pubkey()), &miner.pubkey(), &miner).expect("quote");
    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("a0");
    send(&mut svm, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("a1");

    // Reservation via the lottery, run *before* BASE_TS so each test still starts at BASE_TS with an
    // active reservation (reserved_until = setup_ts + TTL > BASE_TS) — the swap-time assertions are
    // unchanged. Single requester → that requester wins the draw deterministically.
    let setup_ts = BASE_TS - 100;
    set_clock(&mut svm, setup_ts);
    let user = Keypair::new().pubkey();
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), &user), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, setup_ts + POOL_WINDOW_SECS + 1);
    send(&mut svm, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("resolve");
    set_clock(&mut svm, BASE_TS);

    (svm, vals, miner, rent_reserve)
}

fn do_initiate(svm: &mut LiteSVM, vals: &[Keypair], miner: &Pubkey, tx: &str, user: &Pubkey) {
    send(svm, initiate_ix(&vals[0].pubkey(), miner, tx, user, "userSOLaddr"), &vals[0].pubkey(), &vals[0]).expect("i0");
    send(svm, initiate_ix(&vals[1].pubkey(), miner, tx, user, "userSOLaddr"), &vals[1].pubkey(), &vals[1]).expect("i1");
}

/// Reserve a miner via the lottery (open → warp past the window → resolve; sole entrant wins). Leaves
/// the clock advanced past the window; the reservation is active for TTL.
fn do_reserve(svm: &mut LiteSVM, opener: &Keypair, miner: &Pubkey) {
    let now = svm.get_sysvar::<Clock>().unix_timestamp;
    let user = Keypair::new().pubkey();
    send(svm, open_ix(&opener.pubkey(), miner, &user), &opener.pubkey(), opener).expect("open");
    set_clock(svm, now + POOL_WINDOW_SECS + 1);
    send(svm, resolve_ix(&opener.pubkey(), miner), &opener.pubkey(), opener).expect("resolve");
}

fn invariant_holds(svm: &LiteSVM, miner: &Pubkey, rent_reserve: u64) -> bool {
    // Each miner's collateral vault holds only that miner's collateral; revenue is in the treasury.
    collateral_vault_lamports(svm, miner) == rent_reserve + miner_state(svm, miner).collateral
}

#[test]
fn test_initiate_creates_swap() {
    let (mut svm, vals, miner, _rent) = setup();
    let user = Keypair::new().pubkey();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1", &user);

    let key = swap_key("srctx1");
    let a = svm.get_account(&swap_pda(&key)).unwrap();
    let s = Swap::try_deserialize(&mut a.data.as_slice()).unwrap();
    assert_eq!(s.user, user);
    assert_eq!(s.miner, miner.pubkey());
    assert_eq!(s.sol_amount, SOL_AMOUNT);
    // miner quote sourced from the (immutable) reservation
    assert_eq!(s.miner_from_addr, MINER_FROM);
    assert_eq!(s.miner_to_addr, MINER_TO);
    assert_eq!(s.rate, RATE);
    assert_eq!(s.user_to_addr, "userSOLaddr");
    assert_eq!(s.timeout_at, BASE_TS + TIMEOUT_SECS);
    // side effects
    assert!(miner_state(&svm, &miner.pubkey()).has_active_swap);
    assert!(svm.get_account(&tx_pda(&key)).is_some());
}

#[test]
fn test_open_rejected_below_overcollateralization() {
    // Miner holds 2.1 SOL: ≥ min_collateral (1) and ≥ 1.0× the 2 SOL swap, but < 1.10× (2.2 SOL). The
    // over-collateralization gate moved to pool entry (review #1), so `open` rejects it up front —
    // a reservation that vote_initiate could never satisfy is never created, so funds can't strand.
    let under = SOL_AMOUNT + SOL_AMOUNT / 20; // 2.1 SOL (< 2.2 = 1.10×)
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
    send(&mut svm, post_ix(&miner.pubkey(), under), &miner.pubkey(), &miner).expect("post");
    send(&mut svm, set_quote_ix(&miner.pubkey()), &miner.pubkey(), &miner).expect("quote");
    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("a0");
    send(&mut svm, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("a1");

    let user = Keypair::new().pubkey();
    let res = send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey(), &user), &vals[0].pubkey(), &vals[0]);
    assert!(res.is_err(), "open must reject collateral below 1.1× the swap size");
}

#[test]
fn test_initiate_user_mismatch_rejected() {
    let (mut svm, vals, miner, _rent) = setup();
    let user = Keypair::new().pubkey();
    let key = swap_key("srctx1");
    let bad = Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteInitiate {
            swap_key: key,
            from_tx_hash: "srctx1".to_string(),
            from_tx_block: 1,
            user,
            user_from_address: "WRONGaddr".to_string(), // != reservation.from_addr
            user_to_address: "x".to_string(),
        }
        .data(),
        allways_swap_manager::accounts::VoteInitiate {
            validator: vals[0].pubkey(),
            config: config_pda(),
            miner: miner.pubkey(),
            miner_state: miner_pda(&miner.pubkey()),
            reservation: resv_pda(&miner.pubkey()),
            vote_round: vote_pda(REQ_INITIATE, miner.pubkey().as_ref()),
            tx_marker: tx_pda(&key),
            swap: swap_pda(&key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    );
    assert!(send(&mut svm, bad, &vals[0].pubkey(), &vals[0]).is_err(), "user_from mismatch rejected");
}

#[test]
fn test_initiate_hash_binding_user_to() {
    // Two validators differing only in user_to_address → second rejected (closes #2 / #411).
    let (mut svm, vals, miner, _rent) = setup();
    let user = Keypair::new().pubkey();
    send(&mut svm, initiate_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1", &user, "destA"), &vals[0].pubkey(), &vals[0]).expect("i0");
    let mismatched = send(&mut svm, initiate_ix(&vals[1].pubkey(), &miner.pubkey(), "srctx1", &user, "destB"), &vals[1].pubkey(), &vals[1]);
    assert!(mismatched.is_err(), "differing user_to_address must be rejected");
    assert!(!miner_state(&svm, &miner.pubkey()).has_active_swap, "no quorum");
}

#[test]
fn test_fulfill_confirm_fee_and_invariant() {
    let (mut svm, vals, miner, rent) = setup();
    let user = Keypair::new().pubkey();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1", &user);
    assert!(invariant_holds(&svm, &miner.pubkey(), rent));

    // confirm before fulfill must fail
    assert!(confirm_only(&mut svm, &vals[0], &miner.pubkey(), "srctx1").is_err());

    send(&mut svm, fulfill_ix(&miner.pubkey(), "srctx1"), &miner.pubkey(), &miner).expect("fulfill");

    let coll_before = miner_state(&svm, &miner.pubkey()).collateral;
    assert_eq!(miner_state(&svm, &miner.pubkey()).successful_swaps, 0, "no success credit before confirm quorum");
    send(&mut svm, confirm_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("c0");
    send(&mut svm, confirm_ix(&vals[1].pubkey(), &miner.pubkey(), "srctx1"), &vals[1].pubkey(), &vals[1]).expect("c1");

    let fee = SOL_AMOUNT / 100;
    assert_eq!(miner_state(&svm, &miner.pubkey()).collateral, coll_before - fee, "1% fee taken");
    // Quote creation is free; treasury holds the setup's reservation fee plus this swap's 1% confirm fee.
    assert_eq!(
        treasury_total(&svm),
        RESERVATION_FEE_LAMPORTS + fee,
        "fees accrued to treasury"
    );
    assert!(!miner_state(&svm, &miner.pubkey()).has_active_swap);
    assert_eq!(miner_state(&svm, &miner.pubkey()).successful_swaps, 1, "success counter bumped on confirm quorum");
    assert_eq!(miner_state(&svm, &miner.pubkey()).failed_swaps, 0, "no failure on a completed swap");
    assert!(svm.get_account(&swap_pda(&swap_key("srctx1"))).is_none(), "swap closed");
    assert!(invariant_holds(&svm, &miner.pubkey(), rent), "collateral-vault invariant after fee");
}

fn confirm_only(svm: &mut LiteSVM, v: &Keypair, miner: &Pubkey, tx: &str) -> Result<(), String> {
    send(svm, confirm_ix(&v.pubkey(), miner, tx), &v.pubkey(), v)
}

#[test]
fn test_timeout_slash_refund_and_invariant() {
    let (mut svm, vals, miner, rent) = setup();
    let user = Keypair::new();
    svm.airdrop(&user.pubkey(), 1_000_000_000).unwrap();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1", &user.pubkey());

    // not yet timed out
    assert!(timeout_only(&mut svm, &vals[0], &miner.pubkey(), &user.pubkey(), "srctx1").is_err());

    set_clock(&mut svm, BASE_TS + TIMEOUT_SECS + 1);
    let coll_before = miner_state(&svm, &miner.pubkey()).collateral;
    let user_before = lamports(&svm, &user.pubkey());

    send(&mut svm, timeout_ix(&vals[0].pubkey(), &miner.pubkey(), &user.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("t0");
    send(&mut svm, timeout_ix(&vals[1].pubkey(), &miner.pubkey(), &user.pubkey(), "srctx1"), &vals[1].pubkey(), &vals[1]).expect("t1");

    // v2 #4: failed swaps are slashed at 1.10× the swap size, all refunded to the user.
    // collateral (10 SOL) >= 1.1× swap size (2.2 SOL), so the full penalty is taken.
    let slash = SOL_AMOUNT + SOL_AMOUNT / 10; // 1.10× = 2.2 SOL
    assert_eq!(miner_state(&svm, &miner.pubkey()).collateral, coll_before - slash, "collateral slashed 1.1x");
    assert_eq!(lamports(&svm, &user.pubkey()), user_before + slash, "user refunded full 1.1x slash");
    assert!(!miner_state(&svm, &miner.pubkey()).has_active_swap);
    assert_eq!(miner_state(&svm, &miner.pubkey()).failed_swaps, 1, "failure counter bumped on timeout quorum");
    assert_eq!(miner_state(&svm, &miner.pubkey()).successful_swaps, 0, "no success on a timed-out swap");
    assert!(svm.get_account(&swap_pda(&swap_key("srctx1"))).is_none(), "swap closed");
    assert!(invariant_holds(&svm, &miner.pubkey(), rent), "collateral-vault invariant after slash");
}

fn timeout_only(svm: &mut LiteSVM, v: &Keypair, miner: &Pubkey, user: &Pubkey, tx: &str) -> Result<(), String> {
    send(svm, timeout_ix(&v.pubkey(), miner, user, tx), &v.pubkey(), v)
}

#[test]
fn test_replay_guard_survives_completion() {
    let (mut svm, vals, miner, _rent) = setup();
    let user = Keypair::new().pubkey();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1", &user);
    send(&mut svm, fulfill_ix(&miner.pubkey(), "srctx1"), &miner.pubkey(), &miner).expect("fulfill");
    send(&mut svm, confirm_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("c0");
    send(&mut svm, confirm_ix(&vals[1].pubkey(), &miner.pubkey(), "srctx1"), &vals[1].pubkey(), &vals[1]).expect("c1");

    // miner freed → re-reserve via the lottery, then re-initiate with the SAME from_tx_hash → replay rejected.
    do_reserve(&mut svm, &vals[0], &miner.pubkey());
    let replay = send(&mut svm, initiate_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1", &user, "userSOLaddr"), &vals[0].pubkey(), &vals[0]);
    assert!(replay.is_err(), "reused source tx must be rejected even after the swap closed");
}

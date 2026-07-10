// Phase 4 — swap lifecycle: initiate / fulfill / confirm / timeout (LiteSVM, clock-controlled).
//   cargo test -p allways_swap_manager --test test_swap
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    allways_swap_manager::constants::{POOL_WINDOW_SECS, RESERVATION_FEE_LAMPORTS},
    allways_swap_manager::state::{MinerDirectionStats, MinerState, Pool, Reservation, Swap, SwapStatus, Treasury},
    litesvm::LiteSVM,
    solana_hash::Hash,
    solana_keccak_hasher::hashv,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_slot_hashes::SlotHashes,
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
const SOL_AMOUNT: u64 = 2_000_000_000; // 2 SOL swap size (collateral basis)
// BTC→SOL: SOL is the dest leg, and the finalize collateral bind requires collateral_amount == to_amount.
// So TO_AMOUNT (the SOL leg) equals SOL_AMOUNT; FROM_AMOUNT (the BTC leg) keeps to/from ≈ RATE (1.5).
const FROM_AMOUNT: u128 = 1_333_333_333; // source leg (asset-native units); to/from ≈ 1.5
const TO_AMOUNT: u128 = 2_000_000_000; // dest (SOL) leg == collateral basis (bind)
const FROM_TX_BLOCK: u32 = 800_000;
// Fixed taker pinned by the lottery in `setup` (the Swap's user now comes from the reservation).
const LOTTERY_USER: Pubkey = Pubkey::new_from_array([7u8; 32]);

// reservation quote (must be consistent reserve→initiate)
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
fn stats_pda(m: &Pubkey, f: &str, t: &str) -> Pubkey {
    Pubkey::find_program_address(&[b"stats", m.as_ref(), f.as_bytes(), t.as_bytes()], &pid()).0
}
fn pool_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"pool", m.as_ref()], &pid()).0
}
fn swap_pda(key: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"swap", key], &pid()).0
}
fn swap_key(from_tx_hash: &str) -> [u8; 32] {
    hashv(&[from_tx_hash.as_bytes()]).to_bytes()
}

fn set_clock(svm: &mut LiteSVM, ts: i64) {
    let mut clock = svm.get_sysvar::<Clock>();
    clock.unix_timestamp = ts;
    svm.set_sysvar::<Clock>(&clock);
}
/// resolve_pool is two-phase: arm the draw on a future slot, produce it, then draw. See
/// tests/test_reservation.rs for the entropy invariants this protects.
fn arm_and_resolve(svm: &mut LiteSVM, val: &Keypair, miner: &Pubkey) {
    send(svm, resolve_ix(&val.pubkey(), miner), &val.pubkey(), val).expect("arm draw");
    let a = svm.get_account(&pool_pda(miner)).unwrap();
    let seed_slot = Pool::try_deserialize(&mut a.data.as_slice()).unwrap().seed_slot;
    let entries: Vec<(u64, Hash)> = [seed_slot - 1, seed_slot, seed_slot + 1]
        .iter()
        .map(|&s| (s, Hash::new_from_array([s as u8; 32])))
        .collect();
    svm.set_sysvar::<SlotHashes>(&SlotHashes::new(&entries));
    send(svm, resolve_ix(&val.pubkey(), miner), &val.pubkey(), val).expect("resolve");
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
fn set_halted_ix(admin: &Pubkey, halted: bool) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetHalted { halted }.data(),
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
/// Post a quote with caller-chosen payout addresses + rate on the same pair (overwrites in place) —
/// used to prove a post-reservation re-quote cannot redirect an in-flight swap.
fn set_quote_vals_ix(miner: &Pubkey, from_addr: &str, to_addr: &str, rate: u128) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetQuote {
            from_chain: FROM_CHAIN.to_string(),
            to_chain: TO_CHAIN.to_string(),
            miner_from_addr: from_addr.to_string(),
            miner_to_addr: to_addr.to_string(),
            rate,
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
fn open_ix(router: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::OpenOrRequest {
            from_chain: FROM_CHAIN.to_string(),
            to_chain: TO_CHAIN.to_string(),
        }
        .data(),
        allways_swap_manager::accounts::OpenOrRequest {
            router: *router,
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
/// The seat winner fills the reservation with the pinned taker + amounts (BTC→SOL constants).
fn finalize_ix(router: &Pubkey, miner: &Pubkey, user: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::FinalizeReservation {
            user: *user,
            user_from_addr: FROM_ADDR.to_string(),
            user_to_addr: "userSOLaddr".to_string(),
            collateral_amount: SOL_AMOUNT,
            from_amount: FROM_AMOUNT,
            to_amount: TO_AMOUNT,
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
fn claim_ix(caller: &Pubkey, miner: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SubmitSwapClaim {
            swap_key: key,
            from_tx_hash: from_tx_hash.to_string(),
            from_tx_block: FROM_TX_BLOCK,
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
fn initiate_ix(validator: &Pubkey, miner: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteInitiate { swap_key: key }.data(),
        allways_swap_manager::accounts::VoteInitiate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner),
            vote_round: vote_pda(REQ_INITIATE, miner.as_ref()),
            swap: swap_pda(&key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn close_stale_claim_ix(caller: &Pubkey, miner: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::CloseStaleClaim { swap_key: key }.data(),
        allways_swap_manager::accounts::CloseStaleClaim {
            caller: *caller,
            miner: *miner,
            reservation: resv_pda(miner),
            swap: swap_pda(&key),
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
        &allways_swap_manager::instruction::ConfirmSwap {
            swap_key: key,
            from_chain: FROM_CHAIN.to_string(),
            to_chain: TO_CHAIN.to_string(),
        }
        .data(),
        allways_swap_manager::accounts::ConfirmSwap {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            collateral_vault: collateral_vault_pda(miner),
            treasury: treasury_pda(),
            swap: swap_pda(&key),
            direction_stats: stats_pda(miner, FROM_CHAIN, TO_CHAIN),
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
fn direction_stats(svm: &LiteSVM, m: &Pubkey, f: &str, t: &str) -> MinerDirectionStats {
    let a = svm.get_account(&stats_pda(m, f, t)).unwrap();
    MinerDirectionStats::try_deserialize(&mut a.data.as_slice()).unwrap()
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
    let (svm, _admin, vals, miner, rent) = setup_full(collateral);
    (svm, vals, miner, rent)
}

/// As `setup_with_collateral`, but also returns the admin keypair — needed to drive admin-only
/// instructions (e.g. set_halted) in tests.
fn setup_full(collateral: u64) -> (LiteSVM, Keypair, Vec<Keypair>, Keypair, u64) {
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
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("open");
    set_clock(&mut svm, setup_ts + POOL_WINDOW_SECS + 1);
    arm_and_resolve(&mut svm, &vals[0], &miner.pubkey());
    // sole bidder wins → it finalizes the fill (pins LOTTERY_USER + amounts).
    send(&mut svm, finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER), &vals[0].pubkey(), &vals[0]).expect("finalize");
    set_clock(&mut svm, BASE_TS);

    (svm, admin, vals, miner, rent_reserve)
}

/// Claim the source tx on-chain, then attest it to quorum (PendingAttestation → Active).
fn do_initiate(svm: &mut LiteSVM, vals: &[Keypair], miner: &Pubkey, tx: &str) {
    send(svm, claim_ix(&vals[0].pubkey(), miner, tx), &vals[0].pubkey(), &vals[0]).expect("claim");
    send(svm, initiate_ix(&vals[0].pubkey(), miner, tx), &vals[0].pubkey(), &vals[0]).expect("i0");
    send(svm, initiate_ix(&vals[1].pubkey(), miner, tx), &vals[1].pubkey(), &vals[1]).expect("i1");
}

/// Reserve a miner via the lottery (open → warp past the window → resolve; sole entrant wins). Leaves
/// the clock advanced past the window; the reservation is active for TTL.
fn do_reserve(svm: &mut LiteSVM, opener: &Keypair, miner: &Pubkey) {
    let now = svm.get_sysvar::<Clock>().unix_timestamp;
    send(svm, open_ix(&opener.pubkey(), miner), &opener.pubkey(), opener).expect("open");
    set_clock(svm, now + POOL_WINDOW_SECS + 1);
    arm_and_resolve(svm, opener, miner);
    // sole bidder (opener) wins → finalize the fill.
    send(svm, finalize_ix(&opener.pubkey(), miner, &LOTTERY_USER), &opener.pubkey(), opener).expect("finalize");
}

fn invariant_holds(svm: &LiteSVM, miner: &Pubkey, rent_reserve: u64) -> bool {
    // Each miner's collateral vault holds only that miner's collateral; revenue is in the treasury.
    collateral_vault_lamports(svm, miner) == rent_reserve + miner_state(svm, miner).collateral
}

#[test]
fn test_initiate_creates_swap() {
    let (mut svm, vals, miner, _rent) = setup();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");

    let key = swap_key("srctx1");
    let a = svm.get_account(&swap_pda(&key)).unwrap();
    let s = Swap::try_deserialize(&mut a.data.as_slice()).unwrap();
    assert_eq!(s.user, LOTTERY_USER); // pinned by the lottery reservation, not the claimer
    assert_eq!(s.miner, miner.pubkey());
    assert_eq!(s.collateral_amount, SOL_AMOUNT);
    // miner quote sourced from the (immutable) reservation
    assert_eq!(s.miner_from_addr, MINER_FROM);
    assert_eq!(s.miner_to_addr, MINER_TO);
    assert_eq!(s.rate, RATE);
    assert_eq!(s.user_to_addr, "userSOLaddr");
    assert_eq!(s.timeout_at, BASE_TS + TIMEOUT_SECS);
    // side effects
    assert!(miner_state(&svm, &miner.pubkey()).has_active_swap);
}

#[test]
fn test_finalize_rejected_below_overcollateralization() {
    // Miner holds 2.1 SOL: ≥ min_collateral (1) and ≥ 1.0× the 2 SOL swap, but < 1.10× (2.2 SOL). Under
    // two-phase the over-collateralization gate lives at FINALIZE (the amount is only known there), so
    // the bid+draw succeed but the fill is rejected — a reservation vote_initiate could never satisfy is
    // never made LIVE, so funds can't strand.
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
    // bid + draw succeed (no amount known yet)...
    send(&mut svm, open_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("bid");
    set_clock(&mut svm, BASE_TS + POOL_WINDOW_SECS + 1);
    arm_and_resolve(&mut svm, &vals[0], &miner.pubkey());
    // ...but the fill is rejected: 2 SOL × 1.10 = 2.2 SOL > 2.1 SOL held.
    let res = send(&mut svm, finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &user), &vals[0].pubkey(), &vals[0]);
    assert!(res.is_err(), "finalize must reject collateral below 1.1× the swap size");
}

#[test]
fn test_claim_creates_pending() {
    // A claim records the source tx on-chain (PendingAttestation) with the pinned payout, obligates
    // nothing, and leaves the reservation live with its claim slot set.
    let (mut svm, vals, miner, _rent) = setup();
    send(&mut svm, claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("claim");

    let key = swap_key("srctx1");
    let s = Swap::try_deserialize(&mut svm.get_account(&swap_pda(&key)).unwrap().data.as_slice()).unwrap();
    assert_eq!(s.status, SwapStatus::PendingAttestation);
    assert_eq!(s.user, LOTTERY_USER, "payout pinned from the reservation, not the claimer");
    assert_eq!(s.user_to_addr, "userSOLaddr");
    assert_eq!(s.from_tx_hash, "srctx1");
    assert_eq!(s.timeout_at, 0, "no obligation at claim");
    assert!(!miner_state(&svm, &miner.pubkey()).has_active_swap, "claim sets no obligation");
    let r = Reservation::try_deserialize(&mut svm.get_account(&resv_pda(&miner.pubkey())).unwrap().data.as_slice()).unwrap();
    assert_ne!(r.reserved_until, 0, "reservation still live");
    assert_eq!(r.claimed_swap_key, key, "claim slot taken");
    assert!(r.created_at > 0, "resolve_pool stamped created_at (the source-freshness bound)");
}

#[test]
fn test_claim_requires_validator() {
    // The claim is validator-relayed: a non-validator caller is rejected, so there's no anonymous claim
    // to squat the slot / make validators RPC-chase. Participation stays open via the pool draw; only
    // the on-chain relay of the deposit is gated to validators.
    let (mut svm, _vals, miner, _rent) = setup();
    let attacker = Keypair::new();
    svm.airdrop(&attacker.pubkey(), 1_000_000_000).unwrap();
    let res = send(&mut svm, claim_ix(&attacker.pubkey(), &miner.pubkey(), "srctx1"), &attacker.pubkey(), &attacker);
    assert!(res.is_err(), "non-validator claim must be rejected (validator-relay gate)");
}

#[test]
fn test_second_claim_rejected() {
    // One live claim per reservation: a second claim (different tx) is rejected.
    let (mut svm, vals, miner, _rent) = setup();
    send(&mut svm, claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("claim1");
    let second = send(&mut svm, claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx2"), &vals[0].pubkey(), &vals[0]);
    assert!(second.is_err(), "reservation already has a live claim");
}

#[test]
fn test_pending_cannot_be_fulfilled_or_confirmed() {
    // A claim that hasn't been attested is not Active → can't be fulfilled or confirmed.
    let (mut svm, vals, miner, _rent) = setup();
    send(&mut svm, claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("claim");
    assert!(send(&mut svm, fulfill_ix(&miner.pubkey(), "srctx1"), &miner.pubkey(), &miner).is_err(), "pending can't fulfill");
    assert!(confirm_only(&mut svm, &vals[0], &miner.pubkey(), "srctx1").is_err(), "pending can't confirm");
}

#[test]
fn test_stale_claim_reap() {
    // A claim whose reservation lapsed (no attestation) is reapable; rent → caller, slot cleared.
    let (mut svm, vals, miner, _rent) = setup();
    send(&mut svm, claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("claim");
    let key = swap_key("srctx1");
    // can't reap while the reservation is still live
    assert!(send(&mut svm, close_stale_claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).is_err());

    set_clock(&mut svm, BASE_TS + TTL + 1); // reservation expired
    send(&mut svm, close_stale_claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("reap");
    assert!(svm.get_account(&swap_pda(&key)).is_none(), "stale claim closed");
    let r = Reservation::try_deserialize(&mut svm.get_account(&resv_pda(&miner.pubkey())).unwrap().data.as_slice()).unwrap();
    assert_eq!(r.claimed_swap_key, [0u8; 32], "claim slot freed");
}

#[test]
fn test_fulfill_confirm_fee_and_invariant() {
    let (mut svm, vals, miner, rent) = setup();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");
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
    // realized per-direction stats accrued on the completed swap
    let st = direction_stats(&svm, &miner.pubkey(), FROM_CHAIN, TO_CHAIN);
    assert_eq!(st.miner, miner.pubkey());
    assert_eq!(st.from_chain, FROM_CHAIN);
    assert_eq!(st.to_chain, TO_CHAIN);
    assert_eq!(st.completed, 1, "one completed swap");
    assert_eq!(st.total_from_amount, FROM_AMOUNT);
    assert_eq!(st.total_to_amount, TO_AMOUNT);
    // realized VWAP = to/from = 1.5
    assert_eq!(st.total_to_amount * 1_000 / st.total_from_amount, 1_500, "realized VWAP 1.5");
    assert!(svm.get_account(&swap_pda(&swap_key("srctx1"))).is_none(), "swap closed");
    assert!(invariant_holds(&svm, &miner.pubkey(), rent), "collateral-vault invariant after fee");
}

fn confirm_only(svm: &mut LiteSVM, v: &Keypair, miner: &Pubkey, tx: &str) -> Result<(), String> {
    send(svm, confirm_ix(&v.pubkey(), miner, tx), &v.pubkey(), v)
}

#[test]
fn test_timeout_slash_refund_and_invariant() {
    let (mut svm, vals, miner, rent) = setup();
    // the swap's user (refund recipient) is the pinned lottery user
    let user = LOTTERY_USER;
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");

    // not yet timed out
    assert!(timeout_only(&mut svm, &vals[0], &miner.pubkey(), &user, "srctx1").is_err());

    set_clock(&mut svm, BASE_TS + TIMEOUT_SECS + 1);
    let coll_before = miner_state(&svm, &miner.pubkey()).collateral;
    let user_before = lamports(&svm, &user);

    send(&mut svm, timeout_ix(&vals[0].pubkey(), &miner.pubkey(), &user, "srctx1"), &vals[0].pubkey(), &vals[0]).expect("t0");
    send(&mut svm, timeout_ix(&vals[1].pubkey(), &miner.pubkey(), &user, "srctx1"), &vals[1].pubkey(), &vals[1]).expect("t1");

    // v2 #4: failed swaps are slashed at 1.10× the swap size, all refunded to the user.
    // collateral (10 SOL) >= 1.1× swap size (2.2 SOL), so the full penalty is taken.
    let slash = SOL_AMOUNT + SOL_AMOUNT / 10; // 1.10× = 2.2 SOL
    assert_eq!(miner_state(&svm, &miner.pubkey()).collateral, coll_before - slash, "collateral slashed 1.1x");
    assert_eq!(lamports(&svm, &user), user_before + slash, "user refunded full 1.1x slash");
    assert!(!miner_state(&svm, &miner.pubkey()).has_active_swap);
    assert_eq!(miner_state(&svm, &miner.pubkey()).failed_swaps, 1, "failure counter bumped on timeout quorum");
    assert_eq!(miner_state(&svm, &miner.pubkey()).successful_swaps, 0, "no success on a timed-out swap");
    // a timed-out swap accrues NO direction stats (timeout_swap has no stats account)
    assert!(svm.get_account(&stats_pda(&miner.pubkey(), FROM_CHAIN, TO_CHAIN)).is_none(), "no stats on timeout");
    assert!(svm.get_account(&swap_pda(&swap_key("srctx1"))).is_none(), "swap closed");
    assert!(invariant_holds(&svm, &miner.pubkey(), rent), "collateral-vault invariant after slash");
}

fn timeout_only(svm: &mut LiteSVM, v: &Keypair, miner: &Pubkey, user: &Pubkey, tx: &str) -> Result<(), String> {
    send(svm, timeout_ix(&v.pubkey(), miner, user, tx), &v.pubkey(), v)
}

#[test]
fn test_halt_blocks_new_entry_and_lifts_on_unhalt() {
    // PRs 8/482/458: halting the subnet must block NEW entry — post_collateral, vote_activate, and pool
    // open all revert SystemHalted — so no fresh capital or reservations enter while paused. Unhalting
    // restores entry. (In-flight finalization is covered by test_halt_allows_inflight_confirm.)
    let mut svm = LiteSVM::new();
    svm.add_program(pid(), include_bytes!("../../../target/deploy/allways_swap_manager.so")).unwrap();
    set_clock(&mut svm, BASE_TS);
    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, init_ix(&admin.pubkey()), &admin.pubkey(), &admin).expect("init");
    let v = Keypair::new();
    svm.airdrop(&v.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, add_validator_ix(&admin.pubkey(), v.pubkey()), &admin.pubkey(), &admin).expect("add val");

    send(&mut svm, set_halted_ix(&admin.pubkey(), true), &admin.pubkey(), &admin).expect("halt");

    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();
    assert!(
        send(&mut svm, post_ix(&miner.pubkey(), COLLATERAL), &miner.pubkey(), &miner).is_err(),
        "post_collateral must revert SystemHalted while halted"
    );

    // Lift the halt → the same entry now succeeds.
    send(&mut svm, set_halted_ix(&admin.pubkey(), false), &admin.pubkey(), &admin).expect("unhalt");
    send(&mut svm, post_ix(&miner.pubkey(), COLLATERAL), &miner.pubkey(), &miner).expect("post after unhalt");
    send(&mut svm, set_quote_ix(&miner.pubkey()), &miner.pubkey(), &miner).expect("quote");

    // Re-halt → activation entry is blocked too.
    send(&mut svm, set_halted_ix(&admin.pubkey(), true), &admin.pubkey(), &admin).expect("re-halt");
    assert!(
        send(&mut svm, vote_activate_ix(&v.pubkey(), &miner.pubkey()), &v.pubkey(), &v).is_err(),
        "vote_activate must revert SystemHalted while halted"
    );
}

#[test]
fn test_halt_allows_inflight_confirm() {
    // PRs 8/482/458 (precedence half): a halt pauses new entry but must NOT strand an in-flight swap —
    // confirm_swap deliberately ignores `halted`. A swap already Active/Fulfilled when the subnet halts
    // can still reach confirm quorum and complete.
    let (mut svm, admin, vals, miner, rent) = setup_full(COLLATERAL);
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");
    send(&mut svm, fulfill_ix(&miner.pubkey(), "srctx1"), &miner.pubkey(), &miner).expect("fulfill");

    // Halt AFTER the swap is in flight.
    send(&mut svm, set_halted_ix(&admin.pubkey(), true), &admin.pubkey(), &admin).expect("halt");

    // In-flight confirm still reaches quorum while halted.
    send(&mut svm, confirm_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("c0 while halted");
    send(&mut svm, confirm_ix(&vals[1].pubkey(), &miner.pubkey(), "srctx1"), &vals[1].pubkey(), &vals[1]).expect("c1 while halted");
    assert_eq!(miner_state(&svm, &miner.pubkey()).successful_swaps, 1, "in-flight swap confirmed despite halt");
    assert!(svm.get_account(&swap_pda(&swap_key("srctx1"))).is_none(), "swap closed");
    assert!(invariant_holds(&svm, &miner.pubkey(), rent), "vault invariant after halted confirm");
}

#[test]
fn test_swap_terms_frozen_against_post_reservation_requote() {
    // Fund-theft defense (invariant A): once a reservation is resolved, the swap's payout addresses and
    // rate are frozen into the immutable Reservation. A miner that re-posts its quote AFTER reserving —
    // pointing the payout at its own wallet and doubling the rate — must NOT be able to redirect the
    // in-flight swap. submit_swap_claim / vote_initiate read terms from the Reservation, never the live
    // quote (the MinerQuote account isn't even in their Accounts structs). Existing tests can't catch a
    // read-from-live-quote regression because they never make the live quote differ from the reservation.
    // NOTE: the *rate band* (is_executable_rate) that would reject a bogus rate is off-chain by design —
    // the contract stores rate opaquely (set_quote.rs); that gate is tested in tests/test_rate.py.
    let (mut svm, vals, miner, _rent) = setup();
    // Miner mutates its live quote out from under the resolved reservation. Whether or not the contract
    // even permits re-quoting while reserved, the swap must still execute on the frozen terms.
    let _ = send(
        &mut svm,
        set_quote_vals_ix(&miner.pubkey(), "ATTACKER_BTC", "ATTACKER_SOL", RATE * 2),
        &miner.pubkey(),
        &miner,
    );
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");

    let s = Swap::try_deserialize(
        &mut svm.get_account(&swap_pda(&swap_key("srctx1"))).unwrap().data.as_slice(),
    )
    .unwrap();
    assert_eq!(s.rate, RATE, "rate frozen at reservation — not the doubled re-quote");
    assert_eq!(s.miner_to_addr, MINER_TO, "payout addr frozen — not the attacker wallet");
    assert_eq!(s.miner_from_addr, MINER_FROM, "source addr frozen at reservation");
    assert_eq!(s.user, LOTTERY_USER, "taker pinned by the lottery, not the re-quote");
    assert_eq!(s.user_to_addr, "userSOLaddr");
}

#[test]
fn test_confirm_succeeds_after_deadline() {
    // Invariant B: confirm_swap gates only on status == Fulfilled — it has NO deadline check. A payout
    // delivered and marked Fulfilled still confirms even when the confirm quorum lands AFTER timeout_at
    // (slow BTC confirmations). Locks in that nobody adds a deadline gate that would re-break the
    // delivered-past-deadline confirm path (the 262/263/264 family). Mirror of the happy-path confirm
    // test, but with the clock warped well past the deadline before quorum.
    let (mut svm, vals, miner, rent) = setup();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");
    send(&mut svm, fulfill_ix(&miner.pubkey(), "srctx1"), &miner.pubkey(), &miner).expect("fulfill");

    set_clock(&mut svm, BASE_TS + TIMEOUT_SECS + 5_000); // well past timeout_at, before confirm quorum

    let coll_before = miner_state(&svm, &miner.pubkey()).collateral;
    send(&mut svm, confirm_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("c0 past deadline");
    send(&mut svm, confirm_ix(&vals[1].pubkey(), &miner.pubkey(), "srctx1"), &vals[1].pubkey(), &vals[1]).expect("c1 past deadline");

    // Confirmed, not slashed: 1% fee (not a 1.1× slash), success credited, no failure, swap closed.
    let fee = SOL_AMOUNT / 100;
    assert_eq!(miner_state(&svm, &miner.pubkey()).collateral, coll_before - fee, "1% fee, not a slash");
    assert_eq!(miner_state(&svm, &miner.pubkey()).successful_swaps, 1, "success credited past deadline");
    assert_eq!(miner_state(&svm, &miner.pubkey()).failed_swaps, 0, "a delivered swap is not failed");
    assert!(!miner_state(&svm, &miner.pubkey()).has_active_swap);
    assert!(svm.get_account(&swap_pda(&swap_key("srctx1"))).is_none(), "swap closed on confirm");
    assert!(invariant_holds(&svm, &miner.pubkey(), rent), "vault invariant after past-deadline confirm");
}

#[test]
fn test_contract_no_longer_blocks_reused_tx() {
    // A4: the permanent on-chain TxMarker is gone, so the CONTRACT no longer blocks a reused
    // from_tx_hash after the swap closes. Source-replay defense moved to the validator freshness check
    // (the deposit must be mined after Reservation.created_at — an old replayed deposit predates any
    // later reservation), which is off-chain and tested in the validator (Phase B). Here we just prove
    // the marker is truly gone: the re-claim + re-attest now succeeds at the contract level.
    let (mut svm, vals, miner, _rent) = setup();
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");
    send(&mut svm, fulfill_ix(&miner.pubkey(), "srctx1"), &miner.pubkey(), &miner).expect("fulfill");
    send(&mut svm, confirm_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("c0");
    send(&mut svm, confirm_ix(&vals[1].pubkey(), &miner.pubkey(), "srctx1"), &vals[1].pubkey(), &vals[1]).expect("c1");

    // miner freed → re-reserve, re-claim + re-attest the SAME from_tx_hash → now permitted on-chain.
    do_reserve(&mut svm, &vals[0], &miner.pubkey());
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");
    let s = Swap::try_deserialize(&mut svm.get_account(&swap_pda(&swap_key("srctx1"))).unwrap().data.as_slice()).unwrap();
    assert_eq!(s.status, SwapStatus::Active, "contract permits re-attest after close (replay defense now off-chain)");
}

fn run_full_swap(svm: &mut LiteSVM, vals: &[Keypair], miner: &Keypair, tx: &str) {
    do_initiate(svm, vals, &miner.pubkey(), tx);
    send(svm, fulfill_ix(&miner.pubkey(), tx), &miner.pubkey(), miner).expect("fulfill");
    send(svm, confirm_ix(&vals[0].pubkey(), &miner.pubkey(), tx), &vals[0].pubkey(), &vals[0]).expect("c0");
    send(svm, confirm_ix(&vals[1].pubkey(), &miner.pubkey(), tx), &vals[1].pubkey(), &vals[1]).expect("c1");
}

#[test]
fn test_stats_accumulate_same_direction() {
    let (mut svm, vals, miner, _rent) = setup();

    run_full_swap(&mut svm, &vals, &miner, "srctx1");
    assert_eq!(direction_stats(&svm, &miner.pubkey(), FROM_CHAIN, TO_CHAIN).completed, 1);

    // miner freed → re-reserve via the lottery, run a second swap (different source tx) same direction
    do_reserve(&mut svm, &vals[0], &miner.pubkey());
    run_full_swap(&mut svm, &vals, &miner, "srctx2");

    // same PDA, accumulated across both swaps
    let st = direction_stats(&svm, &miner.pubkey(), FROM_CHAIN, TO_CHAIN);
    assert_eq!(st.completed, 2, "two completed swaps accumulate");
    assert_eq!(st.total_from_amount, 2 * FROM_AMOUNT);
    assert_eq!(st.total_to_amount, 2 * TO_AMOUNT);
    assert_eq!(st.total_to_amount * 1_000 / st.total_from_amount, 1_500, "realized VWAP still 1.5");
}

#[test]
fn test_stats_separate_per_direction() {
    let (mut svm, vals, miner, _rent) = setup();
    run_full_swap(&mut svm, &vals, &miner, "srctx1");

    // the forward (BTC→SOL) direction accrued; the reverse-direction PDA was never created — directions
    // do not collide into one stats account.
    assert_eq!(direction_stats(&svm, &miner.pubkey(), FROM_CHAIN, TO_CHAIN).completed, 1);
    assert!(
        svm.get_account(&stats_pda(&miner.pubkey(), TO_CHAIN, FROM_CHAIN)).is_none(),
        "reverse-direction stats PDA not created"
    );
}

/// A reservation CONSUMED by `vote_initiate` (reserved_until back to 0, claim slot cleared) must not be
/// re-fillable — even while `finalize_by` is still in the future.
///
/// `created_at != 0` is the only field distinguishing a consumed reservation from a fresh drawn seat;
/// both carry `reserved_until == 0`. It is the exact guard `close_unfilled_reservation` relies on.
/// Without the same guard here, the seat winner can mint a SECOND live reservation on a miner that
/// already has an active swap — and each fill's 1.10x collateral gate is checked in isolation, so the
/// miner ends up backing two obligations with collateral sized for one.
///
/// Reaching the overlap needs the whole finalize -> claim -> quorum path to land inside the finalize
/// window. At the 60s default that is impractical, which is why `finalize_by` alone has masked this so
/// far. `set-finalize-window` (settable to 300s) widens it, so the invariant is made explicit here
/// rather than left to a timing coincidence.
#[test]
fn test_finalize_refuses_a_reservation_already_consumed_by_initiate() {
    let (mut svm, _admin, vals, miner, _rr) = setup_full(COLLATERAL);

    // setup_full leaves the clock past finalize_by; step back inside the window so this test exercises
    // the created_at guard rather than the deadline.
    let finalize_by = reservation_acct(&svm, &miner.pubkey()).finalize_by;
    set_clock(&mut svm, finalize_by - 5);

    // Drive the filled reservation to an Active swap. vote_initiate consumes it.
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");

    let now = svm.get_sysvar::<Clock>().unix_timestamp;
    let r = reservation_acct(&svm, &miner.pubkey());
    assert_eq!(r.reserved_until, 0, "vote_initiate consumed the reservation");
    assert_eq!(r.claimed_swap_key, [0u8; 32], "and freed the claim slot");
    assert_ne!(r.created_at, 0, "but it WAS filled — not a fresh drawn seat");
    assert!(now <= r.finalize_by, "and the finalize window is still open (the dangerous overlap)");
    assert!(miner_state(&svm, &miner.pubkey()).has_active_swap, "miner is mid-swap");

    let res = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER),
        &vals[0].pubkey(),
        &vals[0],
    );
    assert!(res.is_err(), "a consumed reservation must not be re-filled inside its finalize window");
    assert_eq!(
        reservation_acct(&svm, &miner.pubkey()).reserved_until,
        0,
        "reservation stays consumed — no second live hold on a miner with an active swap"
    );
}

fn reservation_acct(svm: &LiteSVM, miner: &Pubkey) -> Reservation {
    Reservation::try_deserialize(&mut svm.get_account(&resv_pda(miner)).unwrap().data.as_slice()).unwrap()
}

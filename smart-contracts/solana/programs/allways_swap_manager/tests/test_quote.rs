// Phase 8 + W2b — on-chain miner quotes: set_quote / remove_quote (LiteSVM, in-process).
//   cargo test -p allways_swap_manager --test test_quote
//
// A quote is per-(miner, from_chain, to_chain, collateral_chain): a miner advertises its whole book,
// one PDA per direction PER BACKING. These tests cover create, in-place overwrite, multi-pair +
// both-direction coexistence, the mechanical validations (same-chain / empty / too-long), close +
// rent refund, and the W2b backing rules (declarable backings, the activation gate, dual quotes).
//
// Miners are given their activation mask by writing MinerState directly rather than driving a
// validator quorum — the quorum path is covered in test_consensus/test_attestation, and what these
// tests care about is which masks set_quote accepts.
use {
    anchor_lang::{
        prelude::{Clock, Pubkey},
        solana_program::instruction::Instruction,
        AccountDeserialize, AccountSerialize, Discriminator, InstructionData, Space,
        ToAccountMetas,
    },
    allways_swap_manager::state::{MinerQuote, MinerState, Treasury},
    allways_swap_manager::constants::{
        ADDR_LOCK_FEE_LAMPORTS, BACKING_BIT_SOL, BACKING_BIT_TAO, QUOTE_UPDATE_FEE_TIER1_LAMPORTS,
        QUOTE_UPDATE_FEE_TIER1_MAX_SECS, QUOTE_UPDATE_FEE_TIER2_LAMPORTS,
        QUOTE_UPDATE_FEE_TIER2_MAX_SECS, RATE_PRECISION,
    },
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;


fn addr_lock_pda(program_id: &Pubkey, addr: &str) -> Pubkey {
    Pubkey::find_program_address(&[b"addrlock", &allways_swap_manager::addr_lock_key(addr)], program_id).0
}

/// The payout wallet's lock, or None when it is the deposit wallet's (same lock).
fn to_addr_lock_pda(program_id: &Pubkey, from: &str, to: &str) -> Option<Pubkey> {
    (from.to_ascii_lowercase() != to.to_ascii_lowercase()).then(|| addr_lock_pda(program_id, to))
}

fn config_pda(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"config"], program_id).0
}
fn treasury_pda(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"treasury"], program_id).0
}
fn miner_pda(program_id: &Pubkey, miner: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"miner", miner.as_ref()], program_id).0
}
fn quote_pda(
    program_id: &Pubkey,
    miner: &Pubkey,
    from_chain: &str,
    to_chain: &str,
    backing: &str,
) -> Pubkey {
    Pubkey::find_program_address(
        &[
            b"quote",
            miner.as_ref(),
            from_chain.as_bytes(),
            to_chain.as_bytes(),
            backing.as_bytes(),
        ],
        program_id,
    )
    .0
}

/// Plant a MinerState with `mask` lit, so set_quote's activation gate has something to read.
fn activate(svm: &mut LiteSVM, program_id: &Pubkey, miner: &Pubkey, mask: u8) {
    let pda = miner_pda(program_id, miner);
    let bump = Pubkey::find_program_address(&[b"miner", miner.as_ref()], program_id).1;
    let state = MinerState {
        miner: *miner,
        collateral: 0,
        active: mask != 0,
        active_backings: mask,
        has_active_swap: false,
        active_swap_backings: 0,
        busy_until: [0; allways_swap_manager::constants::MAX_BACKING_SLOTS],
        settling_until: [0; allways_swap_manager::constants::MAX_BACKING_SLOTS],
        reserved_collateral: [0; allways_swap_manager::constants::MAX_BACKING_SLOTS],
        deactivation_at: 0,
        successful_swaps: 0,
        failed_swaps: 0,
        bump,
    };
    let mut data = Vec::new();
    state.try_serialize(&mut data).unwrap();
    data.resize(8 + MinerState::INIT_SPACE, 0);
    svm.set_account(
        pda,
        solana_account::Account {
            lamports: 10_000_000,
            data,
            owner: *program_id,
            executable: false,
            rent_epoch: 0,
        },
    )
    .unwrap();
    assert_eq!(&svm.get_account(&pda).unwrap().data[..8], MinerState::DISCRIMINATOR);
}

/// A funded miner with both purses lit — the default fixture, since most cases here are not about
/// which purse is active.
fn dual_miner(svm: &mut LiteSVM, program_id: &Pubkey) -> Keypair {
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();
    activate(svm, program_id, &miner.pubkey(), BACKING_BIT_SOL | BACKING_BIT_TAO);
    miner
}

fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    let blockhash = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &blockhash);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn setup() -> (LiteSVM, Pubkey) {
    let program_id = allways_swap_manager::id();
    let mut svm = LiteSVM::new();
    let bytes = include_bytes!("../../../target/deploy/allways_swap_manager.so");
    svm.add_program(program_id, bytes).unwrap();

    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    let ix = Instruction::new_with_bytes(
        program_id,
        &allways_swap_manager::instruction::Initialize {
            min_collateral: 0,
            max_collateral: 0,
            fulfillment_timeout_secs: 100,
            consensus_threshold_percent: 66,
            min_swap_amount: 1000,
            max_swap_amount: 0,
            reservation_ttl_secs: 1_800,
        }
        .data(),
        allways_swap_manager::accounts::Initialize {
            admin: admin.pubkey(),
            config: config_pda(&program_id),
            treasury: treasury_pda(&program_id),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    );
    send(&mut svm, ix, &admin.pubkey(), &admin).expect("initialize");
    (svm, program_id)
}

/// Post a quote on `(from_chain, to_chain)` backed by `backing`.
#[allow(clippy::too_many_arguments)]
fn set_quote_backed(
    program_id: &Pubkey,
    miner: &Pubkey,
    from_chain: &str,
    to_chain: &str,
    backing: &str,
    miner_from_addr: &str,
    miner_to_addr: &str,
    rate: u128,
    liquidity: u128,
) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::SetQuote {
            from_chain: from_chain.to_string(),
            to_chain: to_chain.to_string(),
            collateral_chain: backing.to_string(),
            miner_from_addr: miner_from_addr.to_string(),
            miner_to_addr: miner_to_addr.to_string(),
            rate,
            liquidity,
            from_addr_lock_key: allways_swap_manager::addr_lock_key(miner_from_addr),
            to_addr_lock_key: allways_swap_manager::addr_lock_key(miner_to_addr),
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *miner,
            miner_state: miner_pda(program_id, miner),
            quote: quote_pda(program_id, miner, from_chain, to_chain, backing),
            from_addr_lock: addr_lock_pda(program_id, miner_from_addr),
            to_addr_lock: to_addr_lock_pda(program_id, miner_from_addr, miner_to_addr),
            treasury: treasury_pda(program_id),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

/// The common case: back the quote with whichever leg is a hub, preferring SOL when both are.
fn default_backing(from_chain: &str, to_chain: &str) -> &'static str {
    if from_chain == "sol" || to_chain == "sol" {
        "sol"
    } else {
        "tao"
    }
}

#[allow(clippy::too_many_arguments)]
fn set_quote_ix(
    program_id: &Pubkey,
    miner: &Pubkey,
    from_chain: &str,
    to_chain: &str,
    miner_from_addr: &str,
    miner_to_addr: &str,
    rate: u128,
    liquidity: u128,
) -> Instruction {
    set_quote_backed(
        program_id,
        miner,
        from_chain,
        to_chain,
        default_backing(from_chain, to_chain),
        miner_from_addr,
        miner_to_addr,
        rate,
        liquidity,
    )
}

fn set_clock(svm: &mut LiteSVM, ts: i64) {
    let mut clock = svm.get_sysvar::<Clock>();
    clock.unix_timestamp = ts;
    svm.set_sysvar::<Clock>(&clock);
}

fn treasury(svm: &LiteSVM, program_id: &Pubkey) -> u64 {
    let a = svm.get_account(&treasury_pda(program_id)).unwrap();
    Treasury::try_deserialize(&mut a.data.as_slice()).unwrap().total
}

fn remove_quote_ix(
    program_id: &Pubkey,
    miner: &Pubkey,
    from_chain: &str,
    to_chain: &str,
) -> Instruction {
    let backing = default_backing(from_chain, to_chain);
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::RemoveQuote {
            from_chain: from_chain.to_string(),
            to_chain: to_chain.to_string(),
            collateral_chain: backing.to_string(),
        }
        .data(),
        allways_swap_manager::accounts::RemoveQuote {
            miner: *miner,
            quote: quote_pda(program_id, miner, from_chain, to_chain, backing),
            treasury: treasury_pda(program_id),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

fn read_quote(svm: &LiteSVM, program_id: &Pubkey, miner: &Pubkey, f: &str, t: &str) -> MinerQuote {
    let a = svm.get_account(&quote_pda(program_id, miner, f, t, default_backing(f, t))).unwrap();
    MinerQuote::try_deserialize(&mut a.data.as_slice()).unwrap()
}

#[test]
fn test_set_quote_creates_pda() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);

    send(
        &mut svm,
        set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", "bc1qsrc", "5Cdst", 340 * RATE_PRECISION, 100),
        &miner.pubkey(),
        &miner,
    )
    .expect("set_quote");

    let q = read_quote(&svm, &program_id, &miner.pubkey(), "btc", "tao");
    assert_eq!(q.miner, miner.pubkey());
    assert_eq!(q.from_chain, "btc");
    assert_eq!(q.to_chain, "tao");
    assert_eq!(q.miner_from_addr, "bc1qsrc");
    assert_eq!(q.miner_to_addr, "5Cdst");
    assert_eq!(q.rate, 340 * RATE_PRECISION);
    assert_eq!(q.liquidity, 100);
    assert!(q.updated_at >= 0);
}

#[test]
fn test_set_quote_overwrites_in_place() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);

    send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", "a", "b", 340 * RATE_PRECISION, 100), &miner.pubkey(), &miner).expect("set1");
    send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", "a", "b", 355 * RATE_PRECISION, 200), &miner.pubkey(), &miner).expect("set2 overwrite");

    let q = read_quote(&svm, &program_id, &miner.pubkey(), "btc", "tao");
    assert_eq!(q.rate, 355 * RATE_PRECISION, "rate updated in place");
    assert_eq!(q.liquidity, 200, "liquidity updated in place");
}

#[test]
fn test_multiple_pairs_and_directions_coexist() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let m = miner.pubkey();

    // Whole book: btc->tao, tao->btc (reverse direction), sol->btc.
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", 340 * RATE_PRECISION, 1), &m, &miner).expect("btc->tao");
    send(&mut svm, set_quote_ix(&program_id, &m, "tao", "btc", "c", "d", 29 * RATE_PRECISION / 10_000, 2), &m, &miner).expect("tao->btc");
    send(&mut svm, set_quote_ix(&program_id, &m, "sol", "btc", "e", "f", 11 * RATE_PRECISION / 10_000, 3), &m, &miner).expect("sol->btc");

    // Each is its own PDA with its own rate; no collision.
    assert_eq!(read_quote(&svm, &program_id, &m, "btc", "tao").rate, 340 * RATE_PRECISION);
    assert_eq!(read_quote(&svm, &program_id, &m, "tao", "btc").rate, 29 * RATE_PRECISION / 10_000);
    assert_eq!(read_quote(&svm, &program_id, &m, "sol", "btc").rate, 11 * RATE_PRECISION / 10_000);
}

#[test]
fn test_same_chain_rejected() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let r = send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "btc", "a", "b", RATE_PRECISION, 1), &miner.pubkey(), &miner);
    assert!(r.is_err(), "from_chain == to_chain must be rejected");
}

#[test]
fn test_uppercase_chain_id_rejected() {
    // "sol"-vs-"SOL" aliasing sizes collateral against the wrong leg and splits quote PDAs;
    // intake must reject any casing but lowercase.
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    for (f, t) in [("BTC", "tao"), ("btc", "TAO"), ("Btc", "tao")] {
        let r = send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), f, t, "a", "b", RATE_PRECISION, 1), &miner.pubkey(), &miner);
        assert!(r.is_err(), "non-lowercase chain id {f}->{t} must be rejected");
    }
}

#[test]
fn test_empty_field_rejected() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let r = send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", "", "b", RATE_PRECISION, 1), &miner.pubkey(), &miner);
    assert!(r.is_err(), "empty miner_from_addr must be rejected");
}

#[test]
fn test_oversized_string_rejected() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let long_addr = "x".repeat(81); // MAX_ADDR_LEN = 80
    let r = send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", &long_addr, "b", RATE_PRECISION, 1), &miner.pubkey(), &miner);
    assert!(r.is_err(), "address over MAX_ADDR_LEN must be rejected");
}

#[test]
fn test_non_ascii_deposit_wallet_rejected() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let r = send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", "BC1Q\u{212a}", "b", RATE_PRECISION, 1), &miner.pubkey(), &miner);
    assert!(r.unwrap_err().contains("NonAlphanumericAddress"), "a non-ASCII deposit wallet would alias a second lock");
}

#[test]
fn test_remove_quote_closes_and_refunds() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let m = miner.pubkey();

    set_clock(&mut svm, 2_000_000);
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", 340 * RATE_PRECISION, 1), &m, &miner).expect("set");
    let before = svm.get_account(&m).unwrap().lamports;
    assert!(svm.get_account(&quote_pda(&program_id, &m, "btc", "tao", "tao")).map(|a| a.lamports > 0).unwrap_or(false));

    // Warp past the decay window so removal is free — keeps this a pure rent-refund check.
    set_clock(&mut svm, 2_000_000 + QUOTE_UPDATE_FEE_TIER2_MAX_SECS + 1);
    send(&mut svm, remove_quote_ix(&program_id, &m, "btc", "tao"), &m, &miner).expect("remove");
    let after = svm.get_account(&m).unwrap().lamports;
    // PDA gone (zero lamports / closed) and rent refunded to miner.
    let closed = svm.get_account(&quote_pda(&program_id, &m, "btc", "tao", "tao")).map(|a| a.lamports == 0).unwrap_or(true);
    assert!(closed, "quote PDA should be closed");
    assert!(after > before, "rent refunded to miner");
}

#[test]
fn test_quote_update_fee_decays() {
    // Creation is free; UPDATES pay a treasury-bound fee that decays to zero the longer the quote has
    // stood (anti-flashing). All amounts land in the treasury.
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let m = miner.pubkey();
    set_clock(&mut svm, 1_000_000); // a real (nonzero) wall clock

    // 1) Creation → no churn fee, only the two new wallet locks.
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", 340 * RATE_PRECISION, 1), &m, &miner).expect("create");
    let locks = 2 * ADDR_LOCK_FEE_LAMPORTS;
    assert_eq!(treasury(&svm, &program_id), locks, "creation pays only the wallet locks");

    // 2) Immediate update (elapsed 0 < 5 min) → tier-1.
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", 341 * RATE_PRECISION, 1), &m, &miner).expect("rapid update");
    let after_t1 = treasury(&svm, &program_id);
    assert_eq!(after_t1, locks + QUOTE_UPDATE_FEE_TIER1_LAMPORTS, "rapid update charges tier-1");

    // 3) Update just past the 5-min window → tier-2.
    set_clock(&mut svm, 1_000_000 + QUOTE_UPDATE_FEE_TIER1_MAX_SECS + 1);
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", 342 * RATE_PRECISION, 1), &m, &miner).expect("tier2 update");
    let after_t2 = treasury(&svm, &program_id);
    assert_eq!(after_t2, after_t1 + QUOTE_UPDATE_FEE_TIER2_LAMPORTS, "5–10 min update charges tier-2");

    // 4) Update past the 10-min window (measured from the previous update) → free.
    set_clock(
        &mut svm,
        1_000_000 + QUOTE_UPDATE_FEE_TIER1_MAX_SECS + 1 + QUOTE_UPDATE_FEE_TIER2_MAX_SECS + 1,
    );
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", 343 * RATE_PRECISION, 1), &m, &miner).expect("free update");
    assert_eq!(treasury(&svm, &program_id), after_t2, "long-standing quote updates for free");
}

#[test]
fn test_remove_quote_charges_churn_fee() {
    // The remove + re-create bypass is closed on the remove side: removing a FRESH quote costs tier-1
    // (same as a rapid in-place update), while a quote that stood past the decay window removes free.
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let m = miner.pubkey();
    set_clock(&mut svm, 1_000_000);

    // Fresh quote (btc/tao), removed immediately → tier-1.
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", 340 * RATE_PRECISION, 1), &m, &miner).expect("create fresh");
    let locks = 2 * ADDR_LOCK_FEE_LAMPORTS;
    assert_eq!(treasury(&svm, &program_id), locks, "creation pays only the wallet locks");
    send(&mut svm, remove_quote_ix(&program_id, &m, "btc", "tao"), &m, &miner).expect("remove fresh");
    assert_eq!(treasury(&svm, &program_id), locks + QUOTE_UPDATE_FEE_TIER1_LAMPORTS, "removing a fresh quote charges tier-1");

    // A different quote (btc/sol) left to stand past the decay window → removes free (treasury unchanged).
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "sol", "a", "b", 341 * RATE_PRECISION, 1), &m, &miner).expect("create stale");
    set_clock(&mut svm, 1_000_000 + QUOTE_UPDATE_FEE_TIER2_MAX_SECS + 1);
    send(&mut svm, remove_quote_ix(&program_id, &m, "btc", "sol"), &m, &miner).expect("remove stale");
    assert_eq!(treasury(&svm, &program_id), locks + QUOTE_UPDATE_FEE_TIER1_LAMPORTS, "a long-standing quote removes free");
}

#[test]
fn test_set_quote_requires_a_miner_state() {
    // W2b reverses the old permissionless rule: a quote names the purse that will answer for it, so a
    // wallet that has never registered has nothing to quote against and is refused at the account level.
    let (mut svm, program_id) = setup();
    let anyone = Keypair::new();
    svm.airdrop(&anyone.pubkey(), 10_000_000_000).unwrap();
    let err = send(
        &mut svm,
        set_quote_ix(&program_id, &anyone.pubkey(), "btc", "tao", "a", "b", 340 * RATE_PRECISION, 1),
        &anyone.pubkey(),
        &anyone,
    )
    .unwrap_err();
    assert!(err.contains("AccountNotInitialized"), "unregistered wallet must be refused, got: {err}");
}

#[test]
fn test_set_quote_requires_that_backing_to_be_active() {
    // A SOL-only miner may not advertise a TAO guarantee, and vice versa — per purse, not per miner.
    let (mut svm, program_id) = setup();
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();
    activate(&mut svm, &program_id, &miner.pubkey(), BACKING_BIT_SOL);

    let err = send(
        &mut svm,
        set_quote_backed(&program_id, &miner.pubkey(), "sol", "tao", "tao", "a", "b", RATE_PRECISION, 1),
        &miner.pubkey(),
        &miner,
    )
    .unwrap_err();
    assert!(err.contains("MinerNotActive"), "TAO-backed quote without the TAO bit, got: {err}");

    // The same direction backed by the purse it DOES hold goes through.
    send(
        &mut svm,
        set_quote_backed(&program_id, &miner.pubkey(), "sol", "tao", "sol", "a", "b", RATE_PRECISION, 1),
        &miner.pubkey(),
        &miner,
    )
    .expect("sol-backed quote on an active SOL purse");
}

#[test]
fn test_backing_must_be_a_hub_on_one_of_the_legs() {
    // Two independent rules, one message each: a leg that is not a hub has no purse, and a hub that is
    // not a leg has nothing to size against.
    let (mut svm, program_id) = setup();
    let m = dual_miner(&mut svm, &program_id);

    let err = send(
        &mut svm,
        set_quote_backed(&program_id, &m.pubkey(), "btc", "tao", "btc", "a", "b", RATE_PRECISION, 1),
        &m.pubkey(),
        &m,
    )
    .unwrap_err();
    assert!(err.contains("BackingNotSupported"), "btc is a leg but not a hub, got: {err}");

    let err = send(
        &mut svm,
        set_quote_backed(&program_id, &m.pubkey(), "btc", "tao", "sol", "a", "b", RATE_PRECISION, 1),
        &m.pubkey(),
        &m,
    )
    .unwrap_err();
    assert!(err.contains("BackingNotInLegs"), "sol is a hub but not a leg here, got: {err}");
}

#[test]
fn test_one_hub_pair_forces_the_hub_and_hub_to_hub_allows_either() {
    // The two halves of the same rule. btc↔tao has exactly one hub leg, so "tao" is the only choice;
    // sol↔tao has two, so both are — and both quotes coexist at distinct PDAs (D2's dual miner).
    let (mut svm, program_id) = setup();
    let m = dual_miner(&mut svm, &program_id);
    let mk = m.pubkey();

    send(&mut svm, set_quote_backed(&program_id, &mk, "btc", "tao", "tao", "a", "b", 340 * RATE_PRECISION, 1), &mk, &m)
        .expect("one-hub pair takes its hub");

    send(&mut svm, set_quote_backed(&program_id, &mk, "sol", "tao", "sol", "a", "b", 10 * RATE_PRECISION, 1), &mk, &m)
        .expect("hub↔hub, sol-backed");
    send(&mut svm, set_quote_backed(&program_id, &mk, "sol", "tao", "tao", "c", "d", 11 * RATE_PRECISION, 2), &mk, &m)
        .expect("hub↔hub, tao-backed");

    // Same miner, same direction, two live offers at different rates — the whole point of the seed.
    let sol_backed = quote_pda(&program_id, &mk, "sol", "tao", "sol");
    let tao_backed = quote_pda(&program_id, &mk, "sol", "tao", "tao");
    assert_ne!(sol_backed, tao_backed, "backing must separate the two quote PDAs");
    let sq = MinerQuote::try_deserialize(&mut svm.get_account(&sol_backed).unwrap().data.as_slice()).unwrap();
    let tq = MinerQuote::try_deserialize(&mut svm.get_account(&tao_backed).unwrap().data.as_slice()).unwrap();
    assert_eq!(sq.collateral_chain, "sol");
    assert_eq!(tq.collateral_chain, "tao");
    assert_eq!(sq.rate, 10 * RATE_PRECISION);
    assert_eq!(tq.rate, 11 * RATE_PRECISION, "the two offers price the guarantee differently");
}

/// `MinerQuote` as the pre-W2b program wrote it — no `collateral_chain`, four seeds.
#[derive(anchor_lang::AnchorSerialize)]
struct MinerQuoteLegacy {
    miner: Pubkey,
    from_chain: String,
    to_chain: String,
    miner_from_addr: String,
    miner_to_addr: String,
    rate: u128,
    liquidity: u128,
    updated_at: i64,
    bump: u8,
}

/// Plant a pre-W2b quote at its legacy (four-seed) address and return that address.
fn plant_legacy_quote(svm: &mut LiteSVM, program_id: &Pubkey, miner: &Pubkey, f: &str, t: &str) -> Pubkey {
    let (pda, bump) = Pubkey::find_program_address(
        &[b"quote", miner.as_ref(), f.as_bytes(), t.as_bytes()],
        program_id,
    );
    let legacy = MinerQuoteLegacy {
        miner: *miner,
        from_chain: f.into(),
        to_chain: t.into(),
        miner_from_addr: "a".into(),
        miner_to_addr: "b".into(),
        rate: RATE_PRECISION,
        liquidity: 0,
        updated_at: 0,
        bump,
    };
    let mut data = MinerQuote::DISCRIMINATOR.to_vec();
    anchor_lang::AnchorSerialize::serialize(&legacy, &mut data).unwrap();
    svm.set_account(
        pda,
        solana_account::Account {
            lamports: 5_000_000,
            data,
            owner: *program_id,
            executable: false,
            rent_epoch: 0,
        },
    )
    .unwrap();
    pda
}

fn close_legacy_ix(program_id: &Pubkey, caller: &Pubkey, miner: &Pubkey, quote: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::CloseLegacyQuote {}.data(),
        allways_swap_manager::accounts::CloseLegacyQuote {
            caller: *caller,
            miner: *miner,
            quote: *quote,
        }
        .to_account_metas(None),
    )
}

#[test]
fn test_legacy_quote_is_reapable_and_rent_goes_to_the_miner() {
    // The W2b seed change orphans every pre-existing quote PDA. Anyone may reap one; the rent goes
    // back to the miner that paid it, so the crank is a favor with nothing in it for the caller.
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let m = miner.pubkey();
    let stranded = plant_legacy_quote(&mut svm, &program_id, &m, "btc", "sol");

    let caller = Keypair::new();
    svm.airdrop(&caller.pubkey(), 10_000_000_000).unwrap();
    let before = svm.get_account(&m).unwrap().lamports;
    let rent = svm.get_account(&stranded).unwrap().lamports;

    send(&mut svm, close_legacy_ix(&program_id, &caller.pubkey(), &m, &stranded), &caller.pubkey(), &caller)
        .expect("permissionless reap");

    assert_eq!(svm.get_account(&stranded).map(|a| a.lamports).unwrap_or(0), 0, "legacy PDA drained");
    assert_eq!(svm.get_account(&m).unwrap().lamports, before + rent, "rent refunded to the miner");
}

#[test]
fn test_the_reaper_cannot_touch_a_live_quote() {
    // The address proof is the whole safety argument: a live quote sits at a five-seed address that
    // no four-seed derivation of its own contents can reproduce, so it is unreachable here.
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let m = miner.pubkey();
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "sol", "a", "b", RATE_PRECISION, 1), &m, &miner)
        .expect("live quote");

    let live = quote_pda(&program_id, &m, "btc", "sol", "sol");
    let caller = Keypair::new();
    svm.airdrop(&caller.pubkey(), 10_000_000_000).unwrap();
    let err = send(&mut svm, close_legacy_ix(&program_id, &caller.pubkey(), &m, &live), &caller.pubkey(), &caller)
        .unwrap_err();
    assert!(err.contains("InvalidAccountForMigration"), "a live quote must be unreapable, got: {err}");
    assert!(svm.get_account(&live).unwrap().lamports > 0, "live quote untouched");
}

#[test]
fn test_the_reaper_refunds_only_the_owning_miner() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let stranded = plant_legacy_quote(&mut svm, &program_id, &miner.pubkey(), "btc", "sol");

    let thief = Keypair::new();
    svm.airdrop(&thief.pubkey(), 10_000_000_000).unwrap();
    let err = send(&mut svm, close_legacy_ix(&program_id, &thief.pubkey(), &thief.pubkey(), &stranded), &thief.pubkey(), &thief)
        .unwrap_err();
    assert!(err.contains("NotMiner"), "rent may only go to the stored miner, got: {err}");
}

// ─── wallet locks ──────────────────────────────────────────────────────────────────────────────

/// set_quote with explicit lock keys / payout-lock account, to drive the mismatch paths.
#[allow(clippy::too_many_arguments)]
fn set_quote_raw(
    program_id: &Pubkey,
    miner: &Pubkey,
    miner_from_addr: &str,
    miner_to_addr: &str,
    from_addr_lock_key: [u8; 32],
    to_addr_lock_key: [u8; 32],
    to_addr_lock: Option<Pubkey>,
) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::SetQuote {
            from_chain: "btc".to_string(),
            to_chain: "sol".to_string(),
            collateral_chain: "sol".to_string(),
            miner_from_addr: miner_from_addr.to_string(),
            miner_to_addr: miner_to_addr.to_string(),
            rate: RATE_PRECISION,
            liquidity: 1,
            from_addr_lock_key,
            to_addr_lock_key,
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *miner,
            miner_state: miner_pda(program_id, miner),
            quote: quote_pda(program_id, miner, "btc", "sol", "sol"),
            from_addr_lock: Pubkey::find_program_address(&[b"addrlock", &from_addr_lock_key], program_id).0,
            to_addr_lock,
            treasury: treasury_pda(program_id),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

fn lock_owner(svm: &LiteSVM, program_id: &Pubkey, addr: &str) -> Option<Pubkey> {
    let a = svm.get_account(&addr_lock_pda(program_id, addr))?;
    Some(allways_swap_manager::state::AddrLock::try_deserialize(&mut a.data.as_slice()).unwrap().owner)
}

fn close_unlocked_ix(program_id: &Pubkey, caller: &Pubkey, q: &MinerQuote, quote: Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::CloseUnlockedQuote {}.data(),
        allways_swap_manager::accounts::CloseUnlockedQuote {
            caller: *caller,
            miner: q.miner,
            quote,
            from_addr_lock: addr_lock_pda(program_id, &q.miner_from_addr),
            to_addr_lock: addr_lock_pda(program_id, &q.miner_to_addr),
        }
        .to_account_metas(None),
    )
}

/// Write a quote account directly, as one posted before wallet locks existed.
fn plant_quote(svm: &mut LiteSVM, program_id: &Pubkey, miner: &Pubkey, f: &str, t: &str, mfrom: &str, mto: &str) -> (Pubkey, MinerQuote) {
    let (pda, bump) = Pubkey::find_program_address(&[b"quote", miner.as_ref(), f.as_bytes(), t.as_bytes(), b"sol"], program_id);
    let q = MinerQuote {
        miner: *miner,
        from_chain: f.into(),
        to_chain: t.into(),
        collateral_chain: "sol".into(),
        miner_from_addr: mfrom.into(),
        miner_to_addr: mto.into(),
        rate: RATE_PRECISION,
        liquidity: 1,
        updated_at: 0,
        bump,
    };
    let mut data = vec![];
    q.try_serialize(&mut data).unwrap();
    svm.set_account(pda, solana_account::Account { lamports: 10_000_000, data, owner: *program_id, executable: false, rent_epoch: 0 })
        .unwrap();
    (pda, q)
}

#[test]
fn test_set_quote_locks_both_wallets_to_poster() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "sol", "bc1qminer", "minerSol", RATE_PRECISION, 1), &miner.pubkey(), &miner)
        .expect("quote");
    assert_eq!(lock_owner(&svm, &program_id, "bc1qminer"), Some(miner.pubkey()));
    assert_eq!(lock_owner(&svm, &program_id, "minerSol"), Some(miner.pubkey()));
}

#[test]
fn test_owner_reuses_its_wallets_across_pairs_and_hubs() {
    // One wallet serves many chains; the lock is per wallet, so the owner may post it anywhere.
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let m = miner.pubkey();
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "sol", "bc1qminer", "minerSol", RATE_PRECISION, 1), &m, &miner).expect("btc->sol");
    send(&mut svm, set_quote_ix(&program_id, &m, "sol", "btc", "minerSol", "bc1qminer", RATE_PRECISION, 1), &m, &miner).expect("sol->btc");
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "BC1QMINER", "minerTao", RATE_PRECISION, 1), &m, &miner).expect("tao hub, re-cased");
}

#[test]
fn test_other_miner_cannot_post_a_taken_wallet() {
    let (mut svm, program_id) = setup();
    let a = dual_miner(&mut svm, &program_id);
    let b = dual_miner(&mut svm, &program_id);
    send(&mut svm, set_quote_ix(&program_id, &a.pubkey(), "btc", "sol", "bc1qminer", "minerSol", RATE_PRECISION, 1), &a.pubkey(), &a)
        .expect("a");
    for (mfrom, mto) in [("bc1qminer", "bOwnSol"), ("bc1qbown", "minerSol"), ("bc1qbown", "MINERSOL")] {
        let e = send(&mut svm, set_quote_ix(&program_id, &b.pubkey(), "btc", "sol", mfrom, mto, RATE_PRECISION, 1), &b.pubkey(), &b)
            .unwrap_err();
        assert!(e.contains("AddressTaken"), "{mfrom}/{mto}: {e}");
    }
}

#[test]
fn test_each_new_wallet_lock_pays_the_fee_once() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let m = miner.pubkey();
    set_clock(&mut svm, 1_000_000);
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "sol", "bc1qminer", "minerSol", RATE_PRECISION, 1), &m, &miner).expect("two new");
    assert_eq!(treasury(&svm, &program_id), 2 * ADDR_LOCK_FEE_LAMPORTS);
    send(&mut svm, set_quote_ix(&program_id, &m, "sol", "btc", "MINERSOL", "bc1qminer", RATE_PRECISION, 1), &m, &miner).expect("both owned");
    assert_eq!(treasury(&svm, &program_id), 2 * ADDR_LOCK_FEE_LAMPORTS, "owned wallets re-post free");
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "bc1qminer", "minerTao", RATE_PRECISION, 1), &m, &miner).expect("one new");
    assert_eq!(treasury(&svm, &program_id), 3 * ADDR_LOCK_FEE_LAMPORTS);
}

#[test]
fn test_same_deposit_and_payout_wallet_takes_one_lock() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "sol", "sameWallet", "SAMEWALLET", RATE_PRECISION, 1), &miner.pubkey(), &miner)
        .expect("one lock serves both legs");
    assert_eq!(lock_owner(&svm, &program_id, "samewallet"), Some(miner.pubkey()));
    assert_eq!(treasury(&svm, &program_id), ADDR_LOCK_FEE_LAMPORTS, "one lock, one fee");
}

#[test]
fn test_payout_lock_may_only_be_omitted_for_the_same_wallet() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let ix = set_quote_raw(
        &program_id,
        &miner.pubkey(),
        "bc1qminer",
        "minerSol",
        allways_swap_manager::addr_lock_key("bc1qminer"),
        allways_swap_manager::addr_lock_key("minerSol"),
        None,
    );
    let e = send(&mut svm, ix, &miner.pubkey(), &miner).unwrap_err();
    assert!(e.contains("MissingAddrLock"), "{e}");
}

#[test]
fn test_lock_keys_must_match_the_posted_wallets() {
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let ix = set_quote_raw(
        &program_id,
        &miner.pubkey(),
        "bc1qminer",
        "minerSol",
        allways_swap_manager::addr_lock_key("bc1qelse"),
        allways_swap_manager::addr_lock_key("minerSol"),
        Some(addr_lock_pda(&program_id, "minerSol")),
    );
    let e = send(&mut svm, ix, &miner.pubkey(), &miner).unwrap_err();
    assert!(e.contains("AddrLockKeyMismatch"), "{e}");
}

#[test]
fn test_wallet_with_whitespace_or_punctuation_rejected() {
    // Such a spelling would key its own lock while naming the same wallet to a lenient reader.
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    for bad in ["bc1qminer\n", " bc1qminer", "bc1q-miner", "0x:abc"] {
        let e = send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "sol", bad, "minerSol", RATE_PRECISION, 1), &miner.pubkey(), &miner)
            .unwrap_err();
        assert!(e.contains("NonAlphanumericAddress"), "{bad:?}: {e}");
    }
}

#[test]
fn test_addr_lock_key_matches_client_vector() {
    // Same vector as the Python client's test, so both derive one lock address.
    let key = allways_swap_manager::addr_lock_key("0xAbCdEf0123456789aBcDeF0123456789AbCdEf01");
    let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, "1f887d4531c70c54739bc5682ffc477b35c5582aeea8c577b4e6c5565b4c9137");
}

#[test]
fn test_close_unlocked_quote_reaps_pre_lock_quotes() {
    let (mut svm, program_id) = setup();
    let owner = dual_miner(&mut svm, &program_id);
    let other = dual_miner(&mut svm, &program_id);
    let caller = dual_miner(&mut svm, &program_id);
    send(&mut svm, set_quote_ix(&program_id, &owner.pubkey(), "btc", "sol", "bc1qowner", "ownerSol", RATE_PRECISION, 1), &owner.pubkey(), &owner)
        .expect("owner quote");

    // A quote from before locks existed, on no lock at all: reaped, rent back to its miner.
    let (pda, q) = plant_quote(&mut svm, &program_id, &other.pubkey(), "sol", "btc", "otherSol", "bc1qother");
    let before = svm.get_balance(&other.pubkey()).unwrap();
    send(&mut svm, close_unlocked_ix(&program_id, &caller.pubkey(), &q, pda), &caller.pubkey(), &caller).expect("reap unlocked");
    assert!(svm.get_account(&pda).is_none_or(|a| a.lamports == 0));
    assert_eq!(svm.get_balance(&other.pubkey()).unwrap(), before + 10_000_000);

    // A pre-lock quote on a wallet another miner owns: reaped.
    let (pda, q) = plant_quote(&mut svm, &program_id, &other.pubkey(), "sol", "btc", "otherSol", "bc1qowner");
    send(&mut svm, close_unlocked_ix(&program_id, &caller.pubkey(), &q, pda), &caller.pubkey(), &caller).expect("reap copied");

    // A quote whose wallets are its miner's: untouchable.
    let q = read_quote(&svm, &program_id, &owner.pubkey(), "btc", "sol");
    let pda = quote_pda(&program_id, &owner.pubkey(), "btc", "sol", "sol");
    let e = send(&mut svm, close_unlocked_ix(&program_id, &caller.pubkey(), &q, pda), &caller.pubkey(), &caller).unwrap_err();
    assert!(e.contains("QuoteLocked"), "{e}");
}

#[test]
fn test_close_unlocked_quote_checks_lock_addresses() {
    let (mut svm, program_id) = setup();
    let owner = dual_miner(&mut svm, &program_id);
    let caller = dual_miner(&mut svm, &program_id);
    send(&mut svm, set_quote_ix(&program_id, &owner.pubkey(), "btc", "sol", "bc1qowner", "ownerSol", RATE_PRECISION, 1), &owner.pubkey(), &owner)
        .expect("owner quote");
    // Passing some other wallet's (empty) lock must not make a locked quote look unlocked.
    let q = read_quote(&svm, &program_id, &owner.pubkey(), "btc", "sol");
    let mut ix = close_unlocked_ix(&program_id, &caller.pubkey(), &q, quote_pda(&program_id, &owner.pubkey(), "btc", "sol", "sol"));
    ix.accounts[3].pubkey = addr_lock_pda(&program_id, "unrelated");
    let e = send(&mut svm, ix, &caller.pubkey(), &caller).unwrap_err();
    assert!(e.contains("AddrLockKeyMismatch"), "{e}");
}

/// Standard-alphabet base64 decode — just enough for `Program data:` log lines.
fn b64(s: &str) -> Vec<u8> {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let (mut acc, mut bits, mut out) = (0u32, 0u32, Vec::new());
    for c in s.bytes().filter(|&c| c != b'=') {
        let v = A.iter().position(|&a| a == c).expect("base64 alphabet") as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}

#[test]
fn test_quote_set_event_carries_the_stored_addresses() {
    use allways_swap_manager::events::QuoteSet;
    use anchor_lang::AnchorDeserialize;

    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let ix = set_quote_ix(
        &program_id,
        &miner.pubkey(),
        "btc",
        "tao",
        "bc1qsrc",
        "5Cdst",
        340 * RATE_PRECISION,
        100,
    );
    let msg = Message::new_with_blockhash(&[ix], Some(&miner.pubkey()), &svm.latest_blockhash());
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[&miner]).unwrap();
    let logs = svm.send_transaction(tx).expect("set_quote").logs;

    let ev = logs
        .iter()
        .filter_map(|l| l.strip_prefix("Program data: "))
        .map(b64)
        .find(|raw| raw.len() > 8 && raw[..8] == QuoteSet::DISCRIMINATOR[..])
        .map(|raw| QuoteSet::deserialize(&mut &raw[8..]).unwrap())
        .expect("QuoteSet in logs");
    assert_eq!((ev.miner_from_addr.as_str(), ev.miner_to_addr.as_str()), ("bc1qsrc", "5Cdst"));
    assert_eq!(ev.rate, 340 * RATE_PRECISION);
}

#[test]
fn test_a_lamports_only_lock_address_is_still_a_new_lock() {
    // Anyone can send lamports to a lock address; that neither claims the wallet nor skips its fee.
    let (mut svm, program_id) = setup();
    let miner = dual_miner(&mut svm, &program_id);
    let m = miner.pubkey();
    svm.airdrop(&addr_lock_pda(&program_id, "bc1qfunded"), 1_000_000).unwrap();
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "sol", "bc1qfunded", "minerSol", RATE_PRECISION, 1), &m, &miner)
        .expect("quote");
    assert_eq!(lock_owner(&svm, &program_id, "bc1qfunded"), Some(m));
    assert_eq!(treasury(&svm, &program_id), 2 * ADDR_LOCK_FEE_LAMPORTS);
}

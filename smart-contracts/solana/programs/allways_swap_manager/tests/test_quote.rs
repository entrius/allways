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
        BACKING_BIT_SOL, BACKING_BIT_TAO, QUOTE_UPDATE_FEE_TIER1_LAMPORTS,
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
        busy_until: 0,
        settling_until: 0,
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
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *miner,
            miner_state: miner_pda(program_id, miner),
            quote: quote_pda(program_id, miner, from_chain, to_chain, backing),
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

    // 1) Creation → free.
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", 340 * RATE_PRECISION, 1), &m, &miner).expect("create");
    assert_eq!(treasury(&svm, &program_id), 0, "creation is free");

    // 2) Immediate update (elapsed 0 < 5 min) → tier-1.
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", 341 * RATE_PRECISION, 1), &m, &miner).expect("rapid update");
    let after_t1 = treasury(&svm, &program_id);
    assert_eq!(after_t1, QUOTE_UPDATE_FEE_TIER1_LAMPORTS, "rapid update charges tier-1");

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
    assert_eq!(treasury(&svm, &program_id), 0, "creation free");
    send(&mut svm, remove_quote_ix(&program_id, &m, "btc", "tao"), &m, &miner).expect("remove fresh");
    assert_eq!(treasury(&svm, &program_id), QUOTE_UPDATE_FEE_TIER1_LAMPORTS, "removing a fresh quote charges tier-1");

    // A different quote (btc/sol) left to stand past the decay window → removes free (treasury unchanged).
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "sol", "a", "b", 341 * RATE_PRECISION, 1), &m, &miner).expect("create stale");
    set_clock(&mut svm, 1_000_000 + QUOTE_UPDATE_FEE_TIER2_MAX_SECS + 1);
    send(&mut svm, remove_quote_ix(&program_id, &m, "btc", "sol"), &m, &miner).expect("remove stale");
    assert_eq!(treasury(&svm, &program_id), QUOTE_UPDATE_FEE_TIER1_LAMPORTS, "a long-standing quote removes free");
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

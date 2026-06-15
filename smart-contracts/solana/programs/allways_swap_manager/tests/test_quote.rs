// Phase 8 — on-chain miner quotes: set_quote / remove_quote (LiteSVM, in-process).
//   cargo test -p allways_swap_manager --test test_quote
//
// set_quote is permissionless and per-(miner, from_chain, to_chain): a miner advertises its whole
// book, one PDA per pair-direction. These tests cover create, in-place overwrite, multi-pair +
// both-direction coexistence, the mechanical validations (same-chain / empty / too-long), close +
// rent refund, and that a wallet with no MinerState can still post.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::instruction::Instruction, AccountDeserialize,
        InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::MinerQuote,
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
fn vault_pda(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"vault"], program_id).0
}
fn quote_pda(program_id: &Pubkey, miner: &Pubkey, from_chain: &str, to_chain: &str) -> Pubkey {
    Pubkey::find_program_address(
        &[b"quote", miner.as_ref(), from_chain.as_bytes(), to_chain.as_bytes()],
        program_id,
    )
    .0
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
            min_swap_amount: 0,
            max_swap_amount: 0,
            reservation_ttl_secs: 1_800,
        }
        .data(),
        allways_swap_manager::accounts::Initialize {
            admin: admin.pubkey(),
            config: config_pda(&program_id),
            vault: vault_pda(&program_id),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    );
    send(&mut svm, ix, &admin.pubkey(), &admin).expect("initialize");
    (svm, program_id)
}

#[allow(clippy::too_many_arguments)]
fn set_quote_ix(
    program_id: &Pubkey,
    miner: &Pubkey,
    from_chain: &str,
    to_chain: &str,
    miner_from_addr: &str,
    miner_to_addr: &str,
    rate: &str,
    liquidity: u128,
) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::SetQuote {
            from_chain: from_chain.to_string(),
            to_chain: to_chain.to_string(),
            miner_from_addr: miner_from_addr.to_string(),
            miner_to_addr: miner_to_addr.to_string(),
            rate: rate.to_string(),
            liquidity,
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *miner,
            quote: quote_pda(program_id, miner, from_chain, to_chain),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

fn remove_quote_ix(
    program_id: &Pubkey,
    miner: &Pubkey,
    from_chain: &str,
    to_chain: &str,
) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::RemoveQuote {
            from_chain: from_chain.to_string(),
            to_chain: to_chain.to_string(),
        }
        .data(),
        allways_swap_manager::accounts::RemoveQuote {
            miner: *miner,
            quote: quote_pda(program_id, miner, from_chain, to_chain),
        }
        .to_account_metas(None),
    )
}

fn read_quote(svm: &LiteSVM, program_id: &Pubkey, miner: &Pubkey, f: &str, t: &str) -> MinerQuote {
    let a = svm.get_account(&quote_pda(program_id, miner, f, t)).unwrap();
    MinerQuote::try_deserialize(&mut a.data.as_slice()).unwrap()
}

#[test]
fn test_set_quote_creates_pda() {
    let (mut svm, program_id) = setup();
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();

    send(
        &mut svm,
        set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", "bc1qsrc", "5Cdst", "340", 100),
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
    assert_eq!(q.rate, "340");
    assert_eq!(q.liquidity, 100);
    assert!(q.updated_at >= 0);
}

#[test]
fn test_set_quote_overwrites_in_place() {
    let (mut svm, program_id) = setup();
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();

    send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", "a", "b", "340", 100), &miner.pubkey(), &miner).expect("set1");
    send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", "a", "b", "355", 200), &miner.pubkey(), &miner).expect("set2 overwrite");

    let q = read_quote(&svm, &program_id, &miner.pubkey(), "btc", "tao");
    assert_eq!(q.rate, "355", "rate updated in place");
    assert_eq!(q.liquidity, 200, "liquidity updated in place");
}

#[test]
fn test_multiple_pairs_and_directions_coexist() {
    let (mut svm, program_id) = setup();
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();
    let m = miner.pubkey();

    // Whole book: btc->tao, tao->btc (reverse direction), sol->btc.
    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", "340", 1), &m, &miner).expect("btc->tao");
    send(&mut svm, set_quote_ix(&program_id, &m, "tao", "btc", "c", "d", "0.0029", 2), &m, &miner).expect("tao->btc");
    send(&mut svm, set_quote_ix(&program_id, &m, "sol", "btc", "e", "f", "0.0011", 3), &m, &miner).expect("sol->btc");

    // Each is its own PDA with its own rate; no collision.
    assert_eq!(read_quote(&svm, &program_id, &m, "btc", "tao").rate, "340");
    assert_eq!(read_quote(&svm, &program_id, &m, "tao", "btc").rate, "0.0029");
    assert_eq!(read_quote(&svm, &program_id, &m, "sol", "btc").rate, "0.0011");
}

#[test]
fn test_same_chain_rejected() {
    let (mut svm, program_id) = setup();
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();
    let r = send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "btc", "a", "b", "1", 1), &miner.pubkey(), &miner);
    assert!(r.is_err(), "from_chain == to_chain must be rejected");
}

#[test]
fn test_empty_field_rejected() {
    let (mut svm, program_id) = setup();
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();
    let r = send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", "", "b", "1", 1), &miner.pubkey(), &miner);
    assert!(r.is_err(), "empty miner_from_addr must be rejected");
}

#[test]
fn test_oversized_string_rejected() {
    let (mut svm, program_id) = setup();
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();
    let long_addr = "x".repeat(81); // MAX_ADDR_LEN = 80
    let r = send(&mut svm, set_quote_ix(&program_id, &miner.pubkey(), "btc", "tao", &long_addr, "b", "1", 1), &miner.pubkey(), &miner);
    assert!(r.is_err(), "address over MAX_ADDR_LEN must be rejected");
}

#[test]
fn test_remove_quote_closes_and_refunds() {
    let (mut svm, program_id) = setup();
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();
    let m = miner.pubkey();

    send(&mut svm, set_quote_ix(&program_id, &m, "btc", "tao", "a", "b", "340", 1), &m, &miner).expect("set");
    let before = svm.get_account(&m).unwrap().lamports;
    assert!(svm.get_account(&quote_pda(&program_id, &m, "btc", "tao")).map(|a| a.lamports > 0).unwrap_or(false));

    send(&mut svm, remove_quote_ix(&program_id, &m, "btc", "tao"), &m, &miner).expect("remove");
    let after = svm.get_account(&m).unwrap().lamports;
    // PDA gone (zero lamports / closed) and rent refunded to miner.
    let closed = svm.get_account(&quote_pda(&program_id, &m, "btc", "tao")).map(|a| a.lamports == 0).unwrap_or(true);
    assert!(closed, "quote PDA should be closed");
    assert!(after > before, "rent refunded to miner");
}

#[test]
fn test_set_quote_is_permissionless() {
    // A fresh wallet that has never posted collateral (no MinerState) can still publish a quote.
    let (mut svm, program_id) = setup();
    let anyone = Keypair::new();
    svm.airdrop(&anyone.pubkey(), 10_000_000_000).unwrap();
    send(
        &mut svm,
        set_quote_ix(&program_id, &anyone.pubkey(), "btc", "tao", "a", "b", "340", 1),
        &anyone.pubkey(),
        &anyone,
    )
    .expect("permissionless set_quote");
    assert_eq!(read_quote(&svm, &program_id, &anyone.pubkey(), "btc", "tao").rate, "340");
}

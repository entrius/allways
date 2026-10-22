// LiteSVM unit tests for the devnet operator lever `reset_miner_strikes`.
//
// The tests load target/deploy/allways_swap_manager.so, so they prove whatever that binary was
// built with: run `cargo test --features devnet-admin` after `anchor build -- --features devnet-admin`
// for the devnet behaviour, and plain `cargo test` after a plain `anchor build` for the mainnet refusal.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::instruction::Instruction, AccountDeserialize,
        AccountSerialize, InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::MinerState,
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;

fn pid() -> Pubkey {
    allways_swap_manager::id()
}
fn config_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"config"], &pid()).0
}
fn treasury_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"treasury"], &pid()).0
}
fn miner_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"miner", m.as_ref()], &pid()).0
}
fn collateral_vault_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"collateral", m.as_ref()], &pid()).0
}

fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    svm.expire_blockhash();
    let bh = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &bh);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn reset_ix(signer: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::ResetMinerStrikes { miner: *miner }.data(),
        allways_swap_manager::accounts::ResetMinerStrikes {
            admin: *signer,
            config: config_pda(),
            miner_state: miner_pda(miner),
        }
        .to_account_metas(None),
    )
}

fn miner_state(svm: &LiteSVM, m: &Pubkey) -> MinerState {
    MinerState::try_deserialize(&mut svm.get_account(&miner_pda(m)).unwrap().data.as_slice()).unwrap()
}

/// Config + admin + one miner whose state carries `strikes` lifetime failures and 5 successes.
fn setup(strikes: u32) -> (LiteSVM, Keypair, Keypair) {
    let mut svm = LiteSVM::new();
    svm.add_program(pid(), include_bytes!("../../../target/deploy/allways_swap_manager.so")).unwrap();

    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    let init = Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Initialize {
            min_collateral: 1_000_000_000,
            max_collateral: 0,
            fulfillment_timeout_secs: 3_600,
            consensus_threshold_percent: 66,
            min_swap_amount: 1000,
            max_swap_amount: 0,
            reservation_ttl_secs: 1_800,
        }
        .data(),
        allways_swap_manager::accounts::Initialize {
            admin: admin.pubkey(),
            config: config_pda(),
            treasury: treasury_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    );
    send(&mut svm, init, &admin.pubkey(), &admin).expect("init");

    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();
    let post = Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::PostCollateral { amount: 10_000_000_000 }.data(),
        allways_swap_manager::accounts::PostCollateral {
            miner: miner.pubkey(),
            config: config_pda(),
            miner_state: miner_pda(&miner.pubkey()),
            collateral_vault: collateral_vault_pda(&miner.pubkey()),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    );
    send(&mut svm, post, &miner.pubkey(), &miner).expect("post");

    // Plant the strikes directly: the counters are only ever written by quorum paths, and the
    // lever's contract is about the field, not about how it got there.
    let pda = miner_pda(&miner.pubkey());
    let mut acct = svm.get_account(&pda).unwrap();
    let mut ms = MinerState::try_deserialize(&mut acct.data.as_slice()).unwrap();
    ms.failed_swaps = strikes;
    ms.successful_swaps = 5;
    let mut data = Vec::with_capacity(acct.data.len());
    ms.try_serialize(&mut data).unwrap();
    data.resize(acct.data.len(), 0);
    acct.data = data;
    svm.set_account(pda, acct).unwrap();
    assert_eq!(miner_state(&svm, &miner.pubkey()).failed_swaps, strikes);

    (svm, admin, miner)
}

#[cfg(feature = "devnet-admin")]
#[test]
fn devnet_build_zeroes_failed_swaps_and_nothing_else() {
    let (mut svm, admin, miner) = setup(2);
    let before = miner_state(&svm, &miner.pubkey());
    send(&mut svm, reset_ix(&admin.pubkey(), &miner.pubkey()), &admin.pubkey(), &admin).expect("reset");
    let after = miner_state(&svm, &miner.pubkey());
    assert_eq!(after.failed_swaps, 0);
    assert_eq!(after.successful_swaps, before.successful_swaps);
    assert_eq!(after.collateral, before.collateral);
    assert_eq!(after.active, before.active);
    // Idempotent: a second reset on a clean miner is a no-op, not an error.
    send(&mut svm, reset_ix(&admin.pubkey(), &miner.pubkey()), &admin.pubkey(), &admin).expect("reset again");
    assert_eq!(miner_state(&svm, &miner.pubkey()).failed_swaps, 0);
}

#[test]
fn only_the_config_admin_may_call_it() {
    let (mut svm, _admin, miner) = setup(2);
    let stranger = Keypair::new();
    svm.airdrop(&stranger.pubkey(), 10_000_000_000).unwrap();
    let err = send(&mut svm, reset_ix(&stranger.pubkey(), &miner.pubkey()), &stranger.pubkey(), &stranger)
        .expect_err("a non-admin signer must be refused");
    assert!(err.contains("2001") || err.contains("ConstraintHasOne"), "unexpected error: {err}");
    assert_eq!(miner_state(&svm, &miner.pubkey()).failed_swaps, 2);
}

#[cfg(not(feature = "devnet-admin"))]
#[test]
fn mainnet_build_refuses_with_devnet_only() {
    let (mut svm, admin, miner) = setup(2);
    let err = send(&mut svm, reset_ix(&admin.pubkey(), &miner.pubkey()), &admin.pubkey(), &admin)
        .expect_err("a build without devnet-admin must refuse");
    assert!(err.contains("DevnetOnly") || err.contains("6"), "unexpected error: {err}");
    assert_eq!(miner_state(&svm, &miner.pubkey()).failed_swaps, 2, "strikes stay monotonic on mainnet");
}

// Phase 1 — collateral deposit/withdraw + vault invariant (LiteSVM, in-process).
//   cargo test -p allways_swap_manager --test test_collateral
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::instruction::Instruction, AccountDeserialize,
        InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::{MinerState, Vault},
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
fn miner_pda(program_id: &Pubkey, miner: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"miner", miner.as_ref()], program_id).0
}

fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    let blockhash = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &blockhash);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn setup(max_collateral: u64) -> (LiteSVM, Pubkey) {
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
            max_collateral,
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

fn post_ix(program_id: &Pubkey, miner: &Pubkey, amount: u64) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::PostCollateral { amount }.data(),
        allways_swap_manager::accounts::PostCollateral {
            miner: *miner,
            config: config_pda(program_id),
            miner_state: miner_pda(program_id, miner),
            vault: vault_pda(program_id),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

fn withdraw_ix(program_id: &Pubkey, miner: &Pubkey, amount: u64) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::WithdrawCollateral { amount }.data(),
        allways_swap_manager::accounts::WithdrawCollateral {
            miner: *miner,
            config: config_pda(program_id),
            miner_state: miner_pda(program_id, miner),
            vault: vault_pda(program_id),
            reservation: None,
        }
        .to_account_metas(None),
    )
}

fn vault_lamports(svm: &LiteSVM, program_id: &Pubkey) -> u64 {
    svm.get_account(&vault_pda(program_id)).unwrap().lamports
}
fn vault_total(svm: &LiteSVM, program_id: &Pubkey) -> u64 {
    let a = svm.get_account(&vault_pda(program_id)).unwrap();
    Vault::try_deserialize(&mut a.data.as_slice()).unwrap().total_collateral
}
fn miner_collateral(svm: &LiteSVM, program_id: &Pubkey, miner: &Pubkey) -> u64 {
    let a = svm.get_account(&miner_pda(program_id, miner)).unwrap();
    MinerState::try_deserialize(&mut a.data.as_slice()).unwrap().collateral
}

#[test]
fn test_post_and_withdraw_maintains_invariant() {
    let (mut svm, program_id) = setup(0); // no cap
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();

    // After init, vault holds only its rent reserve and total_collateral == 0.
    let rent_reserve = vault_lamports(&svm, &program_id);
    assert_eq!(vault_total(&svm, &program_id), 0);

    let dep1 = 2_000_000_000u64;
    send(&mut svm, post_ix(&program_id, &miner.pubkey(), dep1), &miner.pubkey(), &miner).expect("post1");
    assert_eq!(miner_collateral(&svm, &program_id, &miner.pubkey()), dep1);
    assert_eq!(vault_total(&svm, &program_id), dep1);
    assert_eq!(vault_lamports(&svm, &program_id), rent_reserve + dep1, "invariant after deposit 1");

    let dep2 = 1_000_000_000u64;
    send(&mut svm, post_ix(&program_id, &miner.pubkey(), dep2), &miner.pubkey(), &miner).expect("post2");
    assert_eq!(vault_total(&svm, &program_id), dep1 + dep2);
    assert_eq!(vault_lamports(&svm, &program_id), rent_reserve + dep1 + dep2, "invariant after deposit 2");

    let wd = 1_500_000_000u64;
    send(&mut svm, withdraw_ix(&program_id, &miner.pubkey(), wd), &miner.pubkey(), &miner).expect("withdraw");
    let remaining = dep1 + dep2 - wd;
    assert_eq!(miner_collateral(&svm, &program_id, &miner.pubkey()), remaining);
    assert_eq!(vault_total(&svm, &program_id), remaining);
    assert_eq!(vault_lamports(&svm, &program_id), rent_reserve + remaining, "invariant after withdraw");
}

#[test]
fn test_post_collateral_respects_max_cap() {
    let cap = 1_000_000_000u64;
    let (mut svm, program_id) = setup(cap);
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();

    send(&mut svm, post_ix(&program_id, &miner.pubkey(), cap), &miner.pubkey(), &miner).expect("at cap");
    let over = send(&mut svm, post_ix(&program_id, &miner.pubkey(), 1), &miner.pubkey(), &miner);
    assert!(over.is_err(), "deposit over cap should fail");
    assert_eq!(miner_collateral(&svm, &program_id, &miner.pubkey()), cap);
}

#[test]
fn test_withdraw_more_than_balance_fails() {
    let (mut svm, program_id) = setup(0);
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();

    let dep = 1_000_000_000u64;
    send(&mut svm, post_ix(&program_id, &miner.pubkey(), dep), &miner.pubkey(), &miner).expect("post");
    let over = send(&mut svm, withdraw_ix(&program_id, &miner.pubkey(), dep + 1), &miner.pubkey(), &miner);
    assert!(over.is_err(), "over-withdraw should fail");
    assert_eq!(miner_collateral(&svm, &program_id, &miner.pubkey()), dep);
}

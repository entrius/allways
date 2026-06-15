// Phase 8 — per-validator draw weights (LiteSVM, in-process).
//   cargo test -p allways_swap_manager --test test_validator_weight
//
// Weights are the stake-oracle seam consumed only by the Phase 9 lottery draw; consensus stays
// count-based (the regression for that lives in test_consensus.rs, which now runs against the
// ValidatorInfo shape). Here we cover add-with-weight, set_validator_weight, and the unknown-key path.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::instruction::Instruction, AccountDeserialize,
        InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::Config,
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

fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    let blockhash = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &blockhash);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn setup() -> (LiteSVM, Pubkey, Keypair) {
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
    (svm, program_id, admin)
}

fn add_validator_ix(program_id: &Pubkey, admin: &Pubkey, v: Pubkey, weight: u64) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::AddValidator { validator: v, weight }.data(),
        allways_swap_manager::accounts::AdminConfig { admin: *admin, config: config_pda(program_id) }
            .to_account_metas(None),
    )
}

fn set_weight_ix(program_id: &Pubkey, admin: &Pubkey, v: Pubkey, weight: u64) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::SetValidatorWeight { validator: v, weight }.data(),
        allways_swap_manager::accounts::AdminConfig { admin: *admin, config: config_pda(program_id) }
            .to_account_metas(None),
    )
}

fn weight_of(svm: &LiteSVM, program_id: &Pubkey, v: &Pubkey) -> Option<u64> {
    let a = svm.get_account(&config_pda(program_id)).unwrap();
    let cfg = Config::try_deserialize(&mut a.data.as_slice()).unwrap();
    cfg.validators.iter().find(|x| &x.key == v).map(|x| x.weight)
}

#[test]
fn test_add_validator_stores_weight() {
    let (mut svm, program_id, admin) = setup();
    let v = Keypair::new();
    send(&mut svm, add_validator_ix(&program_id, &admin.pubkey(), v.pubkey(), 7), &admin.pubkey(), &admin).expect("add");
    assert_eq!(weight_of(&svm, &program_id, &v.pubkey()), Some(7));
}

#[test]
fn test_set_validator_weight_updates() {
    let (mut svm, program_id, admin) = setup();
    let v = Keypair::new();
    send(&mut svm, add_validator_ix(&program_id, &admin.pubkey(), v.pubkey(), 1), &admin.pubkey(), &admin).expect("add");
    send(&mut svm, set_weight_ix(&program_id, &admin.pubkey(), v.pubkey(), 42), &admin.pubkey(), &admin).expect("set weight");
    assert_eq!(weight_of(&svm, &program_id, &v.pubkey()), Some(42));
}

#[test]
fn test_set_weight_unknown_validator_fails() {
    let (mut svm, program_id, admin) = setup();
    let ghost = Keypair::new();
    let r = send(&mut svm, set_weight_ix(&program_id, &admin.pubkey(), ghost.pubkey(), 5), &admin.pubkey(), &admin);
    assert!(r.is_err(), "setting weight on an unknown validator must fail");
}

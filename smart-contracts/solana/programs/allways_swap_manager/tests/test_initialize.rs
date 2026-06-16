// Phase 0/1 — LiteSVM unit test (in-process SVM, no validator needed).
//   cargo test -p allways_swap_manager test_initialize_creates_config
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::instruction::Instruction, AccountDeserialize,
        InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::{Config, Vault},
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

#[test]
fn test_initialize_creates_config() {
    let program_id = allways_swap_manager::id();
    let admin = Keypair::new();
    let admin_pk = admin.pubkey();

    let mut svm = LiteSVM::new();
    let bytes = include_bytes!("../../../target/deploy/allways_swap_manager.so");
    svm.add_program(program_id, bytes).unwrap();
    svm.airdrop(&admin_pk, 10_000_000_000).unwrap();

    let (config_pda, _) = Pubkey::find_program_address(&[b"config"], &program_id);
    let (vault_pda, _) = Pubkey::find_program_address(&[b"vault"], &program_id);

    let ix = Instruction::new_with_bytes(
        program_id,
        &allways_swap_manager::instruction::Initialize {
            min_collateral: 1_000_000,
            max_collateral: 500_000_000,
            fulfillment_timeout_secs: 12_600,
            consensus_threshold_percent: 66,
            min_swap_amount: 0,
            max_swap_amount: 0,
            reservation_ttl_secs: 1_800,
        }
        .data(),
        allways_swap_manager::accounts::Initialize {
            admin: admin_pk,
            config: config_pda,
            vault: vault_pda,
            system_program: anchor_lang::solana_program::system_program::ID,
        }
        .to_account_metas(None),
    );

    let blockhash = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(&admin_pk), &blockhash);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[&admin]).unwrap();
    let res = svm.send_transaction(tx);
    assert!(res.is_ok(), "initialize should succeed: {:?}", res.err());

    let cfg_acct = svm.get_account(&config_pda).expect("config exists");
    let config = Config::try_deserialize(&mut cfg_acct.data.as_slice()).unwrap();
    assert_eq!(config.admin, admin_pk);
    assert_eq!(config.version, 3);
    assert_eq!(config.min_collateral, 1_000_000);
    assert_eq!(config.max_collateral, 500_000_000);
    assert_eq!(config.fulfillment_timeout_secs, 12_600);
    assert_eq!(config.consensus_threshold_percent, 66);
    assert!(config.validators.is_empty());

    let vault_acct = svm.get_account(&vault_pda).expect("vault exists");
    let vault = Vault::try_deserialize(&mut vault_acct.data.as_slice()).unwrap();
    assert_eq!(vault.total_collateral, 0);
}

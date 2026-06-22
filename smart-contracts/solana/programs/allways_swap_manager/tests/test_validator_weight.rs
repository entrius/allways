// Phase 8 — per-validator draw weights (LiteSVM, in-process).
//   cargo test -p allways_swap_manager --test test_validator_weight
//
// Weights are the stake-oracle seam consumed only by the Phase 9 lottery draw; consensus stays
// count-based (the regression for that lives in test_consensus.rs, which now runs against the
// ValidatorInfo shape). Here we cover add-with-weight, set_validator_weight, and the unknown-key path.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::Config,
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;
const REQ_SET_WEIGHTS: u8 = 8;
const WEIGHTS_MIN_INTERVAL: i64 = 3600;
const BASE_TS: i64 = 1_700_000_000;

fn config_pda(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"config"], program_id).0
}
fn vault_pda(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"vault"], program_id).0
}
fn treasury_pda(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"treasury"], program_id).0
}

fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    svm.expire_blockhash();
    let blockhash = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &blockhash);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn set_clock(svm: &mut LiteSVM, ts: i64) {
    let mut clock = svm.get_sysvar::<Clock>();
    clock.unix_timestamp = ts;
    svm.set_sysvar::<Clock>(&clock);
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
            treasury: treasury_pda(&program_id),
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

// ─── Phase 10: consensus-governed weights (vote_set_weights) ────────────────────

fn weights_round_pda(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[REQ_SET_WEIGHTS]], program_id).0
}

/// init (clock at BASE_TS, threshold 66) + n validators with weight 1. Returns their keypairs.
fn setup_vals(n: usize) -> (LiteSVM, Pubkey, Vec<Keypair>) {
    let (mut svm, program_id, admin) = setup();
    set_clock(&mut svm, BASE_TS);
    let mut vals = Vec::new();
    for _ in 0..n {
        let v = Keypair::new();
        svm.airdrop(&v.pubkey(), 100_000_000_000).unwrap();
        send(&mut svm, add_validator_ix(&program_id, &admin.pubkey(), v.pubkey(), 1), &admin.pubkey(), &admin)
            .expect("add val");
        vals.push(v);
    }
    (svm, program_id, vals)
}

fn vote_weights_ix(program_id: &Pubkey, validator: &Pubkey, weights: Vec<u64>) -> Instruction {
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::VoteSetWeights { weights }.data(),
        allways_swap_manager::accounts::VoteSetWeights {
            validator: *validator,
            config: config_pda(program_id),
            vote_round: weights_round_pda(program_id),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

fn all_weights(svm: &LiteSVM, program_id: &Pubkey) -> Vec<u64> {
    let a = svm.get_account(&config_pda(program_id)).unwrap();
    let cfg = Config::try_deserialize(&mut a.data.as_slice()).unwrap();
    cfg.validators.iter().map(|v| v.weight).collect()
}
fn last_weights_update(svm: &LiteSVM, program_id: &Pubkey) -> i64 {
    let a = svm.get_account(&config_pda(program_id)).unwrap();
    Config::try_deserialize(&mut a.data.as_slice()).unwrap().last_weights_update
}

#[test]
fn test_vote_set_weights_quorum() {
    let (mut svm, program_id, vals) = setup_vals(3); // 2-of-3
    let new = vec![10u64, 20, 30];

    send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), new.clone()), &vals[0].pubkey(), &vals[0]).expect("v0");
    assert_eq!(all_weights(&svm, &program_id), vec![1, 1, 1], "below quorum → unchanged");

    send(&mut svm, vote_weights_ix(&program_id, &vals[1].pubkey(), new.clone()), &vals[1].pubkey(), &vals[1]).expect("v1");
    assert_eq!(all_weights(&svm, &program_id), new, "quorum → weights applied (index-aligned)");
    assert_eq!(last_weights_update(&svm, &program_id), BASE_TS, "cadence stamp set");
}

#[test]
fn test_vote_set_weights_hash_binding() {
    let (mut svm, program_id, vals) = setup_vals(3);
    send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), vec![10, 20, 30]), &vals[0].pubkey(), &vals[0]).expect("v0");
    // second validator submits a DIFFERENT vector → rejected by the bound hash, no quorum.
    let mism = send(&mut svm, vote_weights_ix(&program_id, &vals[1].pubkey(), vec![10, 20, 31]), &vals[1].pubkey(), &vals[1]);
    assert!(mism.is_err(), "divergent weight vectors must not co-count");
    assert_eq!(all_weights(&svm, &program_id), vec![1, 1, 1], "unchanged");
}

#[test]
fn test_vote_set_weights_wrong_length_rejected() {
    let (mut svm, program_id, vals) = setup_vals(3);
    let r = send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), vec![10, 20]), &vals[0].pubkey(), &vals[0]);
    assert!(r.is_err(), "weights length must match the validator set");
}

#[test]
fn test_vote_set_weights_non_validator_rejected() {
    let (mut svm, program_id, _vals) = setup_vals(3);
    let outsider = Keypair::new();
    svm.airdrop(&outsider.pubkey(), 10_000_000_000).unwrap();
    let r = send(&mut svm, vote_weights_ix(&program_id, &outsider.pubkey(), vec![10, 20, 30]), &outsider.pubkey(), &outsider);
    assert!(r.is_err(), "non-validator cannot vote weights");
}

#[test]
fn test_vote_set_weights_cadence_floor() {
    let (mut svm, program_id, vals) = setup_vals(3);
    let first = vec![10u64, 20, 30];

    // First update succeeds (last_weights_update starts at 0).
    send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), first.clone()), &vals[0].pubkey(), &vals[0]).expect("v0");
    send(&mut svm, vote_weights_ix(&program_id, &vals[1].pubkey(), first.clone()), &vals[1].pubkey(), &vals[1]).expect("v1");
    assert_eq!(all_weights(&svm, &program_id), first);

    // Immediate re-vote → rejected by the cadence floor (even the first voter).
    let too_soon = send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), vec![5, 5, 5]), &vals[0].pubkey(), &vals[0]);
    assert!(too_soon.is_err(), "update faster than the floor must be rejected");

    // Warp past the floor → a fresh update succeeds.
    set_clock(&mut svm, BASE_TS + WEIGHTS_MIN_INTERVAL + 1);
    let second = vec![7u64, 8, 9];
    send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), second.clone()), &vals[0].pubkey(), &vals[0]).expect("v0b");
    send(&mut svm, vote_weights_ix(&program_id, &vals[1].pubkey(), second.clone()), &vals[1].pubkey(), &vals[1]).expect("v1b");
    assert_eq!(all_weights(&svm, &program_id), second, "update allowed once the floor elapsed");
}

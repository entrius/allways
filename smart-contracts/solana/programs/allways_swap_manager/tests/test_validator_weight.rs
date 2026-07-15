// Phase 8 — per-validator draw weights (LiteSVM, in-process).
//   cargo test -p allways_swap_manager --test test_validator_weight
//
// Weights are the stake-oracle seam consumed only by the Phase 9 lottery draw; consensus stays
// count-based (the regression for that lives in test_consensus.rs, which now runs against the
// ValidatorInfo shape). Here we cover add-validator-with-weight and the consensus weight path.
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

// ─── Phase 10: consensus-governed weights (vote_set_weights) ────────────────────

/// The round is keyed by the snapshot hash (competing proposals coexist; a junk vote can't freeze
/// the singleton round). Mirrors `consensus::weights_hash`: keccak(REQ || keys || weights LE).
fn weights_round_key(val_keys: &[Pubkey], weights: &[u64]) -> [u8; 32] {
    let infos: Vec<allways_swap_manager::state::ValidatorInfo> = val_keys
        .iter()
        .map(|k| allways_swap_manager::state::ValidatorInfo { key: *k, weight: 0 })
        .collect();
    allways_swap_manager::consensus::weights_hash(&infos, weights)
}

fn weights_round_pda(program_id: &Pubkey, round_key: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[REQ_SET_WEIGHTS], round_key], program_id).0
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

fn vote_weights_ix(
    program_id: &Pubkey,
    validator: &Pubkey,
    val_keys: &[Pubkey],
    weights: Vec<u64>,
) -> Instruction {
    let round_key = weights_round_key(val_keys, &weights);
    Instruction::new_with_bytes(
        *program_id,
        &allways_swap_manager::instruction::VoteSetWeights { weights, round_key }.data(),
        allways_swap_manager::accounts::VoteSetWeights {
            validator: *validator,
            config: config_pda(program_id),
            vote_round: weights_round_pda(program_id, &round_key),
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

/// Validator-set pubkeys in config order (the hash binds keys in this order).
fn val_keys(vals: &[Keypair]) -> Vec<Pubkey> {
    vals.iter().map(|v| v.pubkey()).collect()
}

#[test]
fn test_vote_set_weights_quorum() {
    let (mut svm, program_id, vals) = setup_vals(3); // 2-of-3
    let keys = val_keys(&vals);
    let new = vec![10u64, 20, 30];

    send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), &keys, new.clone()), &vals[0].pubkey(), &vals[0]).expect("v0");
    assert_eq!(all_weights(&svm, &program_id), vec![1, 1, 1], "below quorum → unchanged");

    send(&mut svm, vote_weights_ix(&program_id, &vals[1].pubkey(), &keys, new.clone()), &vals[1].pubkey(), &vals[1]).expect("v1");
    assert_eq!(all_weights(&svm, &program_id), new, "quorum → weights applied (index-aligned)");
    assert_eq!(last_weights_update(&svm, &program_id), BASE_TS, "cadence stamp set");
}

#[test]
fn test_vote_set_weights_divergent_vectors_never_co_count() {
    let (mut svm, program_id, vals) = setup_vals(3);
    let keys = val_keys(&vals);
    send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), &keys, vec![10, 20, 30]), &vals[0].pubkey(), &vals[0]).expect("v0");
    // A different vector lands in its OWN hash-keyed round — accepted, but the two never co-count.
    send(&mut svm, vote_weights_ix(&program_id, &vals[1].pubkey(), &keys, vec![10, 20, 31]), &vals[1].pubkey(), &vals[1])
        .expect("divergent vector opens its own round");
    assert_eq!(all_weights(&svm, &program_id), vec![1, 1, 1], "neither proposal has quorum → unchanged");
}

#[test]
fn test_vote_set_weights_junk_vote_cannot_freeze_updates() {
    // The old singleton round let one junk vote park a wrong hash and block every honest vote for the
    // round TTL (30 min). Hash-keyed rounds: the junk proposal coexists and the honest one completes.
    let (mut svm, program_id, vals) = setup_vals(3);
    let keys = val_keys(&vals);
    send(&mut svm, vote_weights_ix(&program_id, &vals[2].pubkey(), &keys, vec![9, 9, 9]), &vals[2].pubkey(), &vals[2]).expect("junk vote");

    let agreed = vec![10u64, 20, 30];
    send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), &keys, agreed.clone()), &vals[0].pubkey(), &vals[0]).expect("v0");
    send(&mut svm, vote_weights_ix(&program_id, &vals[1].pubkey(), &keys, agreed.clone()), &vals[1].pubkey(), &vals[1]).expect("v1");
    assert_eq!(all_weights(&svm, &program_id), agreed, "honest quorum lands despite the junk proposal");
}

#[test]
fn test_vote_set_weights_round_key_must_match_weights() {
    // The PDA seed must BE the snapshot hash — a mismatched round_key would let a voter route an
    // arbitrary vector into another proposal's round.
    let (mut svm, program_id, vals) = setup_vals(3);
    let keys = val_keys(&vals);
    let lying_key = weights_round_key(&keys, &[1, 2, 3]); // hash of a DIFFERENT vector
    let ix = Instruction::new_with_bytes(
        program_id,
        &allways_swap_manager::instruction::VoteSetWeights { weights: vec![10, 20, 30], round_key: lying_key }.data(),
        allways_swap_manager::accounts::VoteSetWeights {
            validator: vals[0].pubkey(),
            config: config_pda(&program_id),
            vote_round: weights_round_pda(&program_id, &lying_key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    );
    let r = send(&mut svm, ix, &vals[0].pubkey(), &vals[0]);
    assert!(r.is_err(), "round_key != keccak(snapshot) must be rejected");
}

#[test]
fn test_vote_set_weights_wrong_length_rejected() {
    let (mut svm, program_id, vals) = setup_vals(3);
    let keys = val_keys(&vals);
    let r = send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), &keys, vec![10, 20]), &vals[0].pubkey(), &vals[0]);
    assert!(r.is_err(), "weights length must match the validator set");
}

#[test]
fn test_vote_set_weights_non_validator_rejected() {
    let (mut svm, program_id, vals) = setup_vals(3);
    let keys = val_keys(&vals);
    let outsider = Keypair::new();
    svm.airdrop(&outsider.pubkey(), 10_000_000_000).unwrap();
    let r = send(&mut svm, vote_weights_ix(&program_id, &outsider.pubkey(), &keys, vec![10, 20, 30]), &outsider.pubkey(), &outsider);
    assert!(r.is_err(), "non-validator cannot vote weights");
}

#[test]
fn test_vote_set_weights_cadence_floor() {
    let (mut svm, program_id, vals) = setup_vals(3);
    let keys = val_keys(&vals);
    let first = vec![10u64, 20, 30];

    // First update succeeds (last_weights_update starts at 0).
    send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), &keys, first.clone()), &vals[0].pubkey(), &vals[0]).expect("v0");
    send(&mut svm, vote_weights_ix(&program_id, &vals[1].pubkey(), &keys, first.clone()), &vals[1].pubkey(), &vals[1]).expect("v1");
    assert_eq!(all_weights(&svm, &program_id), first);

    // Immediate re-vote → rejected by the cadence floor (even the first voter).
    let too_soon = send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), &keys, vec![5, 5, 5]), &vals[0].pubkey(), &vals[0]);
    assert!(too_soon.is_err(), "update faster than the floor must be rejected");

    // Warp past the floor → a fresh update succeeds.
    set_clock(&mut svm, BASE_TS + WEIGHTS_MIN_INTERVAL + 1);
    let second = vec![7u64, 8, 9];
    send(&mut svm, vote_weights_ix(&program_id, &vals[0].pubkey(), &keys, second.clone()), &vals[0].pubkey(), &vals[0]).expect("v0b");
    send(&mut svm, vote_weights_ix(&program_id, &vals[1].pubkey(), &keys, second.clone()), &vals[1].pubkey(), &vals[1]).expect("v1b");
    assert_eq!(all_weights(&svm, &program_id), second, "update allowed once the floor elapsed");
}

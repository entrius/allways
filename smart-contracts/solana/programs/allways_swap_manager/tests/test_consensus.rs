// Phase 2 — validator-set + consensus activate/deactivate (LiteSVM, in-process).
//   cargo test -p allways_swap_manager --test test_consensus
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::instruction::Instruction, AccountDeserialize,
        InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::MinerState,
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;
const REQ_ACTIVATE: u8 = 0;
const REQ_DEACTIVATE: u8 = 5;

fn pid() -> Pubkey {
    allways_swap_manager::id()
}
fn config_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"config"], &pid()).0
}
fn vault_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"vault"], &pid()).0
}
fn treasury_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"treasury"], &pid()).0
}
fn miner_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"miner", m.as_ref()], &pid()).0
}
fn vote_pda(req: u8, m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[req], m.as_ref()], &pid()).0
}

fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    // Fresh blockhash each send so legitimately-repeated actions (e.g. re-activation) get a
    // distinct tx signature — LiteSVM doesn't advance the blockhash on its own.
    svm.expire_blockhash();
    let bh = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &bh);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn miner_active(svm: &LiteSVM, m: &Pubkey) -> bool {
    let a = svm.get_account(&miner_pda(m)).unwrap();
    MinerState::try_deserialize(&mut a.data.as_slice()).unwrap().active
}

fn init_ix(admin: &Pubkey, min_collateral: u64, threshold: u8) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Initialize {
            min_collateral,
            max_collateral: 0,
            fulfillment_timeout_secs: 100,
            consensus_threshold_percent: threshold,
            min_swap_amount: 0,
            max_swap_amount: 0,
            reservation_ttl_secs: 1_800,
        }
        .data(),
        allways_swap_manager::accounts::Initialize {
            admin: *admin,
            config: config_pda(),
            vault: vault_pda(),
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
        allways_swap_manager::accounts::AdminConfig {
            admin: *admin,
            config: config_pda(),
        }
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
            vault: vault_pda(),
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
            vote_round: vote_pda(REQ_ACTIVATE, miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn vote_deactivate_ix(validator: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteDeactivate {}.data(),
        allways_swap_manager::accounts::VoteDeactivate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            vote_round: vote_pda(REQ_DEACTIVATE, miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn deactivate_ix(miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Deactivate {}.data(),
        allways_swap_manager::accounts::Deactivate {
            miner: *miner,
            miner_state: miner_pda(miner),
        }
        .to_account_metas(None),
    )
}

/// Initialize + whitelist `n` validators, returning the validator keypairs.
fn setup(min_collateral: u64, threshold: u8, n: usize) -> (LiteSVM, Vec<Keypair>) {
    let mut svm = LiteSVM::new();
    svm.add_program(
        pid(),
        include_bytes!("../../../target/deploy/allways_swap_manager.so"),
    )
    .unwrap();

    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, init_ix(&admin.pubkey(), min_collateral, threshold), &admin.pubkey(), &admin)
        .expect("initialize");

    let mut validators = Vec::new();
    for _ in 0..n {
        let v = Keypair::new();
        svm.airdrop(&v.pubkey(), 100_000_000_000).unwrap();
        send(&mut svm, add_validator_ix(&admin.pubkey(), v.pubkey()), &admin.pubkey(), &admin)
            .expect("add validator");
        validators.push(v);
    }
    (svm, validators)
}

fn fund_miner(svm: &mut LiteSVM, collateral: u64) -> Keypair {
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();
    if collateral > 0 {
        send(svm, post_ix(&miner.pubkey(), collateral), &miner.pubkey(), &miner).expect("post");
    } else {
        // still create MinerState (min_collateral is 0 in these cases)
        send(svm, post_ix(&miner.pubkey(), 1), &miner.pubkey(), &miner).expect("post");
    }
    miner
}

#[test]
fn test_quorum_activation() {
    let min_c = 1_000_000_000u64;
    let (mut svm, vals) = setup(min_c, 66, 3); // 2 of 3 needed
    let miner = fund_miner(&mut svm, min_c);

    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("vote 1");
    assert!(!miner_active(&svm, &miner.pubkey()), "one vote is below quorum");

    send(&mut svm, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("vote 2");
    assert!(miner_active(&svm, &miner.pubkey()), "quorum reached → active");
}

#[test]
fn test_activation_requires_min_collateral() {
    let min_c = 5_000_000_000u64;
    let (mut svm, vals) = setup(min_c, 66, 3);
    let miner = fund_miner(&mut svm, 1_000_000_000); // below min
    let res = send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]);
    assert!(res.is_err(), "under-collateralized miner can't be activated");
}

#[test]
fn test_non_validator_cannot_vote() {
    let (mut svm, _vals) = setup(0, 66, 3);
    let miner = fund_miner(&mut svm, 0);
    let outsider = Keypair::new();
    svm.airdrop(&outsider.pubkey(), 100_000_000_000).unwrap();
    let res = send(&mut svm, vote_activate_ix(&outsider.pubkey(), &miner.pubkey()), &outsider.pubkey(), &outsider);
    assert!(res.is_err(), "non-validator vote must fail");
    assert!(!miner_active(&svm, &miner.pubkey()));
}

#[test]
fn test_double_vote_rejected() {
    let (mut svm, vals) = setup(0, 100, 3); // all 3 needed
    let miner = fund_miner(&mut svm, 0);
    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("v0");
    let again = send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]);
    assert!(again.is_err(), "same validator voting twice must fail");
    assert!(!miner_active(&svm, &miner.pubkey()));
}

#[test]
fn test_self_deactivate_then_reactivate_then_vote_deactivate() {
    let (mut svm, vals) = setup(0, 66, 3);
    let miner = fund_miner(&mut svm, 0);

    // activate (2/3)
    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("a0");
    send(&mut svm, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("a1");
    assert!(miner_active(&svm, &miner.pubkey()));

    // miner self-deactivates
    send(&mut svm, deactivate_ix(&miner.pubkey()), &miner.pubkey(), &miner).expect("self-deactivate");
    assert!(!miner_active(&svm, &miner.pubkey()));

    // re-activate (exercises vote-round reuse after reset)
    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("a0b");
    send(&mut svm, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("a1b");
    assert!(miner_active(&svm, &miner.pubkey()));

    // validators force-deactivate (2/3)
    send(&mut svm, vote_deactivate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("d0");
    send(&mut svm, vote_deactivate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("d1");
    assert!(!miner_active(&svm, &miner.pubkey()));
}

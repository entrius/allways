// Phase 3 — reservations: quorum reserve, bounds, hash-binding, cancel, expiry guards (LiteSVM).
//   cargo test -p allways_swap_manager --test test_reservation
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::{MinerState, Reservation},
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;
const REQ_ACTIVATE: u8 = 0;
const REQ_RESERVE: u8 = 1;
const BASE_TS: i64 = 1_700_000_000;
const TTL: i64 = 1_800;

fn pid() -> Pubkey {
    allways_swap_manager::id()
}
fn config_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"config"], &pid()).0
}
fn vault_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"vault"], &pid()).0
}
fn miner_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"miner", m.as_ref()], &pid()).0
}
fn vote_pda(req: u8, m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[req], m.as_ref()], &pid()).0
}
fn resv_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"resv", m.as_ref()], &pid()).0
}

fn set_clock(svm: &mut LiteSVM, ts: i64) {
    let mut clock = svm.get_sysvar::<Clock>();
    clock.unix_timestamp = ts;
    svm.set_sysvar::<Clock>(&clock);
}

fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    svm.expire_blockhash();
    let bh = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &bh);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn init_ix(admin: &Pubkey, min_swap: u64, max_swap: u64, ttl: i64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Initialize {
            min_collateral: 0,
            max_collateral: 0,
            fulfillment_timeout_secs: 100,
            consensus_threshold_percent: 66,
            min_swap_amount: min_swap,
            max_swap_amount: max_swap,
            reservation_ttl_secs: ttl,
        }
        .data(),
        allways_swap_manager::accounts::Initialize {
            admin: *admin,
            config: config_pda(),
            vault: vault_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn add_validator_ix(admin: &Pubkey, v: Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::AddValidator { validator: v, weight: 1 }.data(),
        allways_swap_manager::accounts::AdminConfig { admin: *admin, config: config_pda() }
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
#[allow(clippy::too_many_arguments)]
fn vote_reserve_ix(
    validator: &Pubkey,
    miner: &Pubkey,
    from_addr: &str,
    from_chain: &str,
    to_chain: &str,
    sol_amount: u64,
    from_amount: u128,
    to_amount: u128,
) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteReserve {
            from_addr: from_addr.to_string(),
            from_chain: from_chain.to_string(),
            to_chain: to_chain.to_string(),
            sol_amount,
            from_amount,
            to_amount,
            // pinned miner quote (v2 #1a) — fixed here; the dedicated binding test varies the rate
            miner_from_addr: "minerBTCaddr".to_string(),
            miner_to_addr: "minerSOLaddr".to_string(),
            rate: "1.5".to_string(),
        }
        .data(),
        allways_swap_manager::accounts::VoteReserve {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            vote_round: vote_pda(REQ_RESERVE, miner),
            reservation: resv_pda(miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn cancel_ix(admin: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::CancelReservation {}.data(),
        allways_swap_manager::accounts::CancelReservation {
            admin: *admin,
            config: config_pda(),
            miner: *miner,
            reservation: resv_pda(miner),
            vote_round: vote_pda(REQ_RESERVE, miner),
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
            reservation: Some(resv_pda(miner)),
        }
        .to_account_metas(None),
    )
}

fn reservation(svm: &LiteSVM, miner: &Pubkey) -> Reservation {
    let a = svm.get_account(&resv_pda(miner)).unwrap();
    Reservation::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn is_active(svm: &LiteSVM, miner: &Pubkey) -> bool {
    let a = svm.get_account(&miner_pda(miner)).unwrap();
    MinerState::try_deserialize(&mut a.data.as_slice()).unwrap().active
}

/// init + 3 validators + a funded, active miner. Clock at BASE_TS.
fn setup(min_swap: u64, max_swap: u64) -> (LiteSVM, Keypair, Vec<Keypair>, Keypair) {
    let mut svm = LiteSVM::new();
    svm.add_program(pid(), include_bytes!("../../../target/deploy/allways_swap_manager.so")).unwrap();
    set_clock(&mut svm, BASE_TS);

    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, init_ix(&admin.pubkey(), min_swap, max_swap, TTL), &admin.pubkey(), &admin).expect("init");

    let mut vals = Vec::new();
    for _ in 0..3 {
        let v = Keypair::new();
        svm.airdrop(&v.pubkey(), 100_000_000_000).unwrap();
        send(&mut svm, add_validator_ix(&admin.pubkey(), v.pubkey()), &admin.pubkey(), &admin).expect("add val");
        vals.push(v);
    }

    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, post_ix(&miner.pubkey(), 10_000_000_000), &miner.pubkey(), &miner).expect("post");
    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("a0");
    send(&mut svm, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("a1");
    (svm, admin, vals, miner)
}

#[test]
fn test_quorum_reserve() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let amt = 2_000_000_000u64;

    send(&mut svm, vote_reserve_ix(&vals[0].pubkey(), &miner.pubkey(), "bc1quser", "BTC", "SOL", amt, 100_000, 0), &vals[0].pubkey(), &vals[0]).expect("r0");
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, 0, "below quorum → not reserved");

    send(&mut svm, vote_reserve_ix(&vals[1].pubkey(), &miner.pubkey(), "bc1quser", "BTC", "SOL", amt, 100_000, 0), &vals[1].pubkey(), &vals[1]).expect("r1");
    let r = reservation(&svm, &miner.pubkey());
    assert_eq!(r.reserved_until, BASE_TS + TTL, "reserved on quorum");
    assert_eq!(r.sol_amount, amt);
    assert_eq!(r.from_addr, "bc1quser");
    assert_eq!(r.from_chain, "BTC");
    assert_eq!(r.to_chain, "SOL");
    // pinned miner quote (v2 #1a) — captured immutably at reserve time
    assert_eq!(r.miner_from_addr, "minerBTCaddr");
    assert_eq!(r.miner_to_addr, "minerSOLaddr");
    assert_eq!(r.rate, "1.5");
}

/// The pinned quote (rate/addresses) is part of the bound hash: two validators differing only in
/// `rate` must not both count toward quorum (v2 #1a — closes the rate-swing / address-theft hole).
#[test]
fn test_reserve_quote_binding() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let amt = 2_000_000_000u64;
    let mk_ix = |validator: &Pubkey, rate: &str| {
        Instruction::new_with_bytes(
            pid(),
            &allways_swap_manager::instruction::VoteReserve {
                from_addr: "u".to_string(),
                from_chain: "BTC".to_string(),
                to_chain: "SOL".to_string(),
                sol_amount: amt,
                from_amount: 1,
                to_amount: 0,
                miner_from_addr: "minerBTCaddr".to_string(),
                miner_to_addr: "minerSOLaddr".to_string(),
                rate: rate.to_string(),
            }
            .data(),
            allways_swap_manager::accounts::VoteReserve {
                validator: *validator,
                config: config_pda(),
                miner: miner.pubkey(),
                miner_state: miner_pda(&miner.pubkey()),
                vote_round: vote_pda(REQ_RESERVE, &miner.pubkey()),
                reservation: resv_pda(&miner.pubkey()),
                system_program: SYSTEM_PROGRAM,
            }
            .to_account_metas(None),
        )
    };
    send(&mut svm, mk_ix(&vals[0].pubkey(), "1.5"), &vals[0].pubkey(), &vals[0]).expect("v0");
    let mismatched = send(&mut svm, mk_ix(&vals[1].pubkey(), "2.0"), &vals[1].pubkey(), &vals[1]);
    assert!(mismatched.is_err(), "differing rate on the same round must be rejected");
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, 0, "no quorum");
}

#[test]
fn test_reserve_amount_bounds() {
    let (mut svm, _admin, vals, miner) = setup(1_000_000_000, 5_000_000_000);
    let below = send(&mut svm, vote_reserve_ix(&vals[0].pubkey(), &miner.pubkey(), "u", "BTC", "SOL", 500_000_000, 1, 0), &vals[0].pubkey(), &vals[0]);
    assert!(below.is_err(), "below min rejected");
    let above = send(&mut svm, vote_reserve_ix(&vals[0].pubkey(), &miner.pubkey(), "u", "BTC", "SOL", 9_000_000_000, 1, 0), &vals[0].pubkey(), &vals[0]);
    assert!(above.is_err(), "above max rejected");
}

#[test]
fn test_reserve_requires_active_miner() {
    let (mut svm, _admin, vals, _miner) = setup(0, 0);
    // a different miner that posted collateral but was never activated
    let inactive = Keypair::new();
    svm.airdrop(&inactive.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, post_ix(&inactive.pubkey(), 1_000_000_000), &inactive.pubkey(), &inactive).expect("post");
    let res = send(&mut svm, vote_reserve_ix(&vals[0].pubkey(), &inactive.pubkey(), "u", "BTC", "SOL", 1, 1, 0), &vals[0].pubkey(), &vals[0]);
    assert!(res.is_err(), "inactive miner cannot be reserved");
}

#[test]
fn test_reserve_hash_binding() {
    // Two validators vote the same miner/round with DIFFERENT amounts → the second must be rejected
    // by the bound-hash. This is the load-bearing property reservations add.
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    send(&mut svm, vote_reserve_ix(&vals[0].pubkey(), &miner.pubkey(), "u", "BTC", "SOL", 2_000_000_000, 1, 0), &vals[0].pubkey(), &vals[0]).expect("v0");
    let mismatched = send(&mut svm, vote_reserve_ix(&vals[1].pubkey(), &miner.pubkey(), "u", "BTC", "SOL", 3_000_000_000, 1, 0), &vals[1].pubkey(), &vals[1]);
    assert!(mismatched.is_err(), "differing terms on the same round must be rejected (VoteHashMismatch)");
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, 0, "no quorum reached");
}

#[test]
fn test_cancel_then_reserve_again() {
    let (mut svm, admin, vals, miner) = setup(0, 0);
    let amt = 2_000_000_000u64;
    send(&mut svm, vote_reserve_ix(&vals[0].pubkey(), &miner.pubkey(), "u", "BTC", "SOL", amt, 1, 0), &vals[0].pubkey(), &vals[0]).expect("v0");
    send(&mut svm, vote_reserve_ix(&vals[1].pubkey(), &miner.pubkey(), "u", "BTC", "SOL", amt, 1, 0), &vals[1].pubkey(), &vals[1]).expect("v1");
    assert!(reservation(&svm, &miner.pubkey()).reserved_until > 0);

    send(&mut svm, cancel_ix(&admin.pubkey(), &miner.pubkey()), &admin.pubkey(), &admin).expect("cancel");
    assert_eq!(reservation(&svm, &miner.pubkey()).reserved_until, 0, "cancel clears reservation");

    // re-reserve works after cancel
    send(&mut svm, vote_reserve_ix(&vals[0].pubkey(), &miner.pubkey(), "u2", "BTC", "SOL", amt, 1, 0), &vals[0].pubkey(), &vals[0]).expect("v0b");
    send(&mut svm, vote_reserve_ix(&vals[1].pubkey(), &miner.pubkey(), "u2", "BTC", "SOL", amt, 1, 0), &vals[1].pubkey(), &vals[1]).expect("v1b");
    assert_eq!(reservation(&svm, &miner.pubkey()).from_addr, "u2");
}

#[test]
fn test_reservation_blocks_deactivate_until_expiry() {
    let (mut svm, _admin, vals, miner) = setup(0, 0);
    let amt = 2_000_000_000u64;
    send(&mut svm, vote_reserve_ix(&vals[0].pubkey(), &miner.pubkey(), "u", "BTC", "SOL", amt, 1, 0), &vals[0].pubkey(), &vals[0]).expect("v0");
    send(&mut svm, vote_reserve_ix(&vals[1].pubkey(), &miner.pubkey(), "u", "BTC", "SOL", amt, 1, 0), &vals[1].pubkey(), &vals[1]).expect("v1");
    assert!(is_active(&svm, &miner.pubkey()));

    // blocked while reserved
    let blocked = send(&mut svm, deactivate_ix(&miner.pubkey()), &miner.pubkey(), &miner);
    assert!(blocked.is_err(), "cannot self-deactivate while reserved");

    // warp past the reservation expiry → now allowed
    set_clock(&mut svm, BASE_TS + TTL + 1);
    send(&mut svm, deactivate_ix(&miner.pubkey()), &miner.pubkey(), &miner).expect("deactivate after expiry");
    assert!(!is_active(&svm, &miner.pubkey()), "miner deactivated once reservation expired");
}

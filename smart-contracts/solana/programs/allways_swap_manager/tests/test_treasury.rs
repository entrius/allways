// Phase 6 — admin treasury withdrawal (LiteSVM). Accrues a fee via a full swap+confirm, then
// withdraws it; checks the admin guard and the over-withdraw guard.
//   cargo test -p allways_swap_manager --test test_treasury
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::Vault,
    litesvm::LiteSVM,
    solana_keccak_hasher::hashv,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

const SYS: Pubkey = anchor_lang::solana_program::system_program::ID;
const BASE_TS: i64 = 1_700_000_000;
const SOL_AMOUNT: u64 = 2_000_000_000;

fn pid() -> Pubkey {
    allways_swap_manager::id()
}
fn cfg() -> Pubkey {
    Pubkey::find_program_address(&[b"config"], &pid()).0
}
fn vault_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"vault"], &pid()).0
}
fn miner_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"miner", m.as_ref()], &pid()).0
}
fn vote_pda(req: u8, key: &[u8]) -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[req], key], &pid()).0
}
fn resv_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"resv", m.as_ref()], &pid()).0
}
fn swap_pda(k: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"swap", k], &pid()).0
}
fn tx_pda(k: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"tx", k], &pid()).0
}
fn skey(tx: &str) -> [u8; 32] {
    hashv(&[tx.as_bytes()]).to_bytes()
}

fn set_clock(svm: &mut LiteSVM, ts: i64) {
    let mut c = svm.get_sysvar::<Clock>();
    c.unix_timestamp = ts;
    svm.set_sysvar::<Clock>(&c);
}
fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, s: &Keypair) -> Result<(), String> {
    svm.expire_blockhash();
    let bh = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &bh);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[s]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}
fn vault(svm: &LiteSVM) -> Vault {
    let a = svm.get_account(&vault_pda()).unwrap();
    Vault::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn lam(svm: &LiteSVM, p: &Pubkey) -> u64 {
    svm.get_account(p).map(|a| a.lamports).unwrap_or(0)
}

fn withdraw_ix(admin: &Pubkey, recipient: &Pubkey, amount: u64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::WithdrawTreasury { amount }.data(),
        allways_swap_manager::accounts::WithdrawTreasury {
            admin: *admin,
            config: cfg(),
            vault: vault_pda(),
            recipient: *recipient,
        }
        .to_account_metas(None),
    )
}

/// Full flow that accrues one fee into the treasury; returns (svm, admin, fee, vault_rent_reserve).
fn setup_with_fee() -> (LiteSVM, Keypair, u64, u64) {
    let mut svm = LiteSVM::new();
    svm.add_program(pid(), include_bytes!("../../../target/deploy/allways_swap_manager.so")).unwrap();
    set_clock(&mut svm, BASE_TS);

    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    // init: min_collateral 1 SOL, threshold 66, ttl 1800, timeout 3600
    send(&mut svm, Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Initialize {
            min_collateral: 1_000_000_000, max_collateral: 0, fulfillment_timeout_secs: 3_600,
            consensus_threshold_percent: 66, min_swap_amount: 0, max_swap_amount: 0, reservation_ttl_secs: 1_800,
        }.data(),
        allways_swap_manager::accounts::Initialize { admin: admin.pubkey(), config: cfg(), vault: vault_pda(), system_program: SYS }.to_account_metas(None),
    ), &admin.pubkey(), &admin).expect("init");
    let rent_reserve = lam(&svm, &vault_pda());

    let mut vals = Vec::new();
    for _ in 0..3 {
        let v = Keypair::new();
        svm.airdrop(&v.pubkey(), 100_000_000_000).unwrap();
        send(&mut svm, Instruction::new_with_bytes(pid(),
            &allways_swap_manager::instruction::AddValidator { validator: v.pubkey() }.data(),
            allways_swap_manager::accounts::AdminConfig { admin: admin.pubkey(), config: cfg() }.to_account_metas(None),
        ), &admin.pubkey(), &admin).expect("add val");
        vals.push(v);
    }

    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, Instruction::new_with_bytes(pid(),
        &allways_swap_manager::instruction::PostCollateral { amount: 10_000_000_000 }.data(),
        allways_swap_manager::accounts::PostCollateral { miner: miner.pubkey(), config: cfg(), miner_state: miner_pda(&miner.pubkey()), vault: vault_pda(), system_program: SYS }.to_account_metas(None),
    ), &miner.pubkey(), &miner).expect("post");

    let activate = |svm: &mut LiteSVM, v: &Keypair| {
        send(svm, Instruction::new_with_bytes(pid(),
            &allways_swap_manager::instruction::VoteActivate {}.data(),
            allways_swap_manager::accounts::VoteActivate { validator: v.pubkey(), config: cfg(), miner: miner.pubkey(), miner_state: miner_pda(&miner.pubkey()), vote_round: vote_pda(0, miner.pubkey().as_ref()), system_program: SYS }.to_account_metas(None),
        ), &v.pubkey(), v).expect("activate");
    };
    activate(&mut svm, &vals[0]);
    activate(&mut svm, &vals[1]);

    let reserve = |svm: &mut LiteSVM, v: &Keypair| {
        send(svm, Instruction::new_with_bytes(pid(),
            &allways_swap_manager::instruction::VoteReserve {
                from_addr: "userBTC".to_string(), from_chain: "BTC".to_string(), to_chain: "SOL".to_string(),
                sol_amount: SOL_AMOUNT, from_amount: 1, to_amount: 0,
                miner_from_addr: "mBTC".to_string(), miner_to_addr: "mSOL".to_string(), rate: "1".to_string(),
            }.data(),
            allways_swap_manager::accounts::VoteReserve { validator: v.pubkey(), config: cfg(), miner: miner.pubkey(), miner_state: miner_pda(&miner.pubkey()), vote_round: vote_pda(1, miner.pubkey().as_ref()), reservation: resv_pda(&miner.pubkey()), system_program: SYS }.to_account_metas(None),
        ), &v.pubkey(), v).expect("reserve");
    };
    reserve(&mut svm, &vals[0]);
    reserve(&mut svm, &vals[1]);

    let key = skey("tx1");
    let user = Keypair::new().pubkey();
    let initiate = |svm: &mut LiteSVM, v: &Keypair| {
        send(svm, Instruction::new_with_bytes(pid(),
            &allways_swap_manager::instruction::VoteInitiate { swap_key: key, from_tx_hash: "tx1".to_string(), from_tx_block: 1, user, user_from_address: "userBTC".to_string(), user_to_address: "userSOL".to_string() }.data(),
            allways_swap_manager::accounts::VoteInitiate { validator: v.pubkey(), config: cfg(), miner: miner.pubkey(), miner_state: miner_pda(&miner.pubkey()), reservation: resv_pda(&miner.pubkey()), vote_round: vote_pda(2, miner.pubkey().as_ref()), tx_marker: tx_pda(&key), swap: swap_pda(&key), system_program: SYS }.to_account_metas(None),
        ), &v.pubkey(), v).expect("initiate");
    };
    initiate(&mut svm, &vals[0]);
    initiate(&mut svm, &vals[1]);

    send(&mut svm, Instruction::new_with_bytes(pid(),
        &allways_swap_manager::instruction::MarkFulfilled { swap_key: key, to_tx_hash: "d".to_string(), to_tx_block: 1 }.data(),
        allways_swap_manager::accounts::MarkFulfilled { miner: miner.pubkey(), swap: swap_pda(&key) }.to_account_metas(None),
    ), &miner.pubkey(), &miner).expect("fulfill");

    let confirm = |svm: &mut LiteSVM, v: &Keypair| {
        send(svm, Instruction::new_with_bytes(pid(),
            &allways_swap_manager::instruction::ConfirmSwap { swap_key: key }.data(),
            allways_swap_manager::accounts::ConfirmSwap { validator: v.pubkey(), config: cfg(), miner: miner.pubkey(), miner_state: miner_pda(&miner.pubkey()), vault: vault_pda(), swap: swap_pda(&key), vote_round: vote_pda(6, &key), system_program: SYS }.to_account_metas(None),
        ), &v.pubkey(), v).expect("confirm");
    };
    confirm(&mut svm, &vals[0]);
    confirm(&mut svm, &vals[1]);

    (svm, admin, SOL_AMOUNT / 100, rent_reserve)
}

#[test]
fn test_withdraw_treasury_happy_path() {
    let (mut svm, admin, fee, rent) = setup_with_fee();
    assert_eq!(vault(&svm).treasury_total, fee, "fee accrued");

    let recipient = Keypair::new().pubkey();
    let before = lam(&svm, &recipient);
    send(&mut svm, withdraw_ix(&admin.pubkey(), &recipient, fee), &admin.pubkey(), &admin).expect("withdraw");

    assert_eq!(lam(&svm, &recipient), before + fee, "recipient received fees");
    let v = vault(&svm);
    assert_eq!(v.treasury_total, 0, "treasury drained");
    // invariant still holds (only collateral lamports + rent remain in the vault)
    assert_eq!(lam(&svm, &vault_pda()), rent + v.total_collateral + v.treasury_total);
}

#[test]
fn test_non_admin_cannot_withdraw() {
    let (mut svm, _admin, fee, _rent) = setup_with_fee();
    let outsider = Keypair::new();
    svm.airdrop(&outsider.pubkey(), 1_000_000_000).unwrap();
    let res = send(&mut svm, withdraw_ix(&outsider.pubkey(), &outsider.pubkey(), fee), &outsider.pubkey(), &outsider);
    assert!(res.is_err(), "non-admin withdrawal rejected");
    assert_eq!(vault(&svm).treasury_total, fee, "treasury untouched");
}

#[test]
fn test_cannot_overdraw_treasury() {
    let (mut svm, admin, fee, _rent) = setup_with_fee();
    let recipient = Keypair::new().pubkey();
    let res = send(&mut svm, withdraw_ix(&admin.pubkey(), &recipient, fee + 1), &admin.pubkey(), &admin);
    assert!(res.is_err(), "withdrawing more than treasury rejected");
    assert_eq!(vault(&svm).treasury_total, fee, "treasury untouched");
}

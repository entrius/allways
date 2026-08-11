// W2 — the BondAttestation layer: quorum'd attestation writes, the global heartbeat fuse,
// busy-until-settled entry lock, per-hub activation, and the v3 migration cranks (LiteSVM).
//   cargo test -p allways_swap_manager --test test_attestation
//
// As in test_backing.rs, no instruction can pin a non-"sol" backing yet (quote-level declaration is
// W2b), so tests that exercise a TAO-backed swap write the backing straight into the account.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, AccountSerialize, AnchorSerialize, Discriminator, InstructionData, Space,
        ToAccountMetas,
    },
    allways_swap_manager::constants::{
        required_collateral, ATTEST_MAX_AGE_SECS, BACKING_BIT_SOL, BACKING_BIT_TAO, CONFIG_VERSION,
        POOL_WINDOW_SECS, SETTLEMENT_GRACE_SECS, TAO_MIN_COLLATERAL_RAO,
    },
    allways_swap_manager::state::{BondAttestation, Config, MinerState, Pool, Reservation, Swap, ValidatorInfo},
    litesvm::LiteSVM,
    solana_account::Account,
    solana_hash::Hash,
    solana_keccak_hasher::hashv,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_slot_hashes::SlotHashes,
    solana_transaction::versioned::VersionedTransaction,
};

/// Every pre-W2b fixture pair has a SOL leg, so "sol" is the backing they all declare.
const BACKING: &str = "sol";
const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;
const SLOT_HASHES_ID: Pubkey = Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111");
const REQ_ACTIVATE: u8 = 0;
const REQ_INITIATE: u8 = 2;
const REQ_DEACTIVATE: u8 = 5;
const REQ_TIMEOUT: u8 = 7;
const REQ_SET_ATTESTATION: u8 = 9;
const REQ_ATTEST_HEARTBEAT: u8 = 10;
const BASE_TS: i64 = 1_700_000_000;
const TTL: i64 = 1_800;
const TIMEOUT_SECS: i64 = 3_600;
const MIN_COLLATERAL: u64 = 1_000_000_000; // 1 SOL
const COLLATERAL: u64 = 10_000_000_000; // 10 SOL
const SOL_AMOUNT: u64 = 2_000_000_000; // 2 SOL
// 0.5 TAO, in rao — inside the deployed [0.1 τ, 1 τ] band, so these tests exercise the backing
// guards rather than the size bounds.
const TAO_AMOUNT: u128 = 500_000_000;
const FROM_TX_BLOCK: u32 = 800_000;
const LOTTERY_USER: Pubkey = Pubkey::new_from_array([7u8; 32]);
const TAO: &str = "tao";
const SOL: &str = "sol";

// Hub↔hub pair: "sol" backing binds to from_amount, "tao" backing to to_amount.
const HUB_FROM: &str = "sol";
const HUB_TO: &str = "tao";
const SPOKE_FROM: &str = "btc";
const SPOKE_TO: &str = "sol";
const MINER_FROM: &str = "minerSrcAddr";
const MINER_TO: &str = "minerDstAddr";
const RATE: u128 = 1_500_000_000_000_000_000;

fn pid() -> Pubkey {
    allways_swap_manager::id()
}
fn config_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"config"], &pid()).0
}
fn treasury_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"treasury"], &pid()).0
}
fn collateral_vault_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"collateral", m.as_ref()], &pid()).0
}
fn miner_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"miner", m.as_ref()], &pid()).0
}
fn attest_pda(m: &Pubkey, chain: &str) -> Pubkey {
    Pubkey::find_program_address(&[b"attest", m.as_ref(), chain.as_bytes()], &pid()).0
}
fn vote_pda(req: u8, key: &[u8]) -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[req], key], &pid()).0
}
fn attest_round_pda(m: &Pubkey, chain: &str) -> Pubkey {
    Pubkey::find_program_address(
        &[b"vote", &[REQ_SET_ATTESTATION], m.as_ref(), chain.as_bytes()],
        &pid(),
    )
    .0
}
fn resv_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"resv", m.as_ref()], &pid()).0
}
fn quote_pda(m: &Pubkey, f: &str, t: &str, b: &str) -> Pubkey {
    Pubkey::find_program_address(
        &[b"quote", m.as_ref(), f.as_bytes(), t.as_bytes(), b.as_bytes()],
        &pid(),
    )
    .0
}
fn pool_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"pool", m.as_ref()], &pid()).0
}
fn swap_pda(key: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"swap", key], &pid()).0
}
fn swap_key(from_tx_hash: &str) -> [u8; 32] {
    hashv(&[from_tx_hash.as_bytes()]).to_bytes()
}

fn set_clock(svm: &mut LiteSVM, ts: i64) {
    let mut clock = svm.get_sysvar::<Clock>();
    clock.unix_timestamp = ts;
    svm.set_sysvar::<Clock>(&clock);
}
fn now_ts(svm: &LiteSVM) -> i64 {
    svm.get_sysvar::<Clock>().unix_timestamp
}

fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    svm.expire_blockhash();
    let bh = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &bh);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}

fn init_ix(admin: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Initialize {
            min_collateral: MIN_COLLATERAL,
            max_collateral: 0,
            fulfillment_timeout_secs: TIMEOUT_SECS,
            consensus_threshold_percent: 66,
            min_swap_amount: 1000,
            max_swap_amount: 0,
            reservation_ttl_secs: TTL,
        }
        .data(),
        allways_swap_manager::accounts::Initialize {
            admin: *admin,
            config: config_pda(),
            treasury: treasury_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn admin_ix(admin: &Pubkey, data: Vec<u8>) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &data,
        allways_swap_manager::accounts::AdminConfig { admin: *admin, config: config_pda() }
            .to_account_metas(None),
    )
}
fn add_validator_ix(admin: &Pubkey, v: Pubkey) -> Instruction {
    admin_ix(admin, allways_swap_manager::instruction::AddValidator { validator: v, weight: 1 }.data())
}
fn post_ix(miner: &Pubkey, amount: u64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::PostCollateral { amount }.data(),
        allways_swap_manager::accounts::PostCollateral {
            miner: *miner,
            config: config_pda(),
            miner_state: miner_pda(miner),
            collateral_vault: collateral_vault_pda(miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn set_quote_ix(miner: &Pubkey, f: &str, t: &str) -> Instruction {
    set_quote_backed_ix(miner, f, t, BACKING)
}
fn set_quote_backed_ix(miner: &Pubkey, f: &str, t: &str, b: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetQuote {
            from_chain: f.to_string(),
            to_chain: t.to_string(),
            collateral_chain: b.to_string(),
            miner_from_addr: MINER_FROM.to_string(),
            miner_to_addr: MINER_TO.to_string(),
            rate: RATE,
            liquidity: 1_000,
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *miner,
            miner_state: miner_pda(miner),
            quote: quote_pda(miner, f, t, b),
            treasury: treasury_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn activate_ix(validator: &Pubkey, miner: &Pubkey, backing: &str, attestation: Option<Pubkey>) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteActivate { backing: backing.to_string() }.data(),
        allways_swap_manager::accounts::VoteActivate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            vote_round: vote_pda(REQ_ACTIVATE, miner.as_ref()),
            attestation,
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn deactivate_ix(validator: &Pubkey, miner: &Pubkey, backing: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteDeactivate { backing: backing.to_string() }.data(),
        allways_swap_manager::accounts::VoteDeactivate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            vote_round: vote_pda(REQ_DEACTIVATE, miner.as_ref()),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn attest_ix(
    validator: &Pubkey,
    miner: &Pubkey,
    chain: &str,
    effective_balance: u64,
    locked: bool,
    epoch: u64,
) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteSetAttestation {
            chain: chain.to_string(),
            effective_balance,
            locked,
            epoch,
        }
        .data(),
        allways_swap_manager::accounts::VoteSetAttestation {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            attestation: attest_pda(miner, chain),
            vote_round: attest_round_pda(miner, chain),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn heartbeat_ix(validator: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteAttestHeartbeat {}.data(),
        allways_swap_manager::accounts::VoteAttestHeartbeat {
            validator: *validator,
            config: config_pda(),
            vote_round: vote_pda(REQ_ATTEST_HEARTBEAT, config_pda().as_ref()),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn open_ix(router: &Pubkey, miner: &Pubkey, f: &str, t: &str) -> Instruction {
    open_backed_ix(router, miner, f, t, BACKING)
}
/// A bid naming the quote's backing — the attestation account rides along for anything but "sol".
fn open_backed_ix(router: &Pubkey, miner: &Pubkey, f: &str, t: &str, b: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::OpenOrRequest {
            from_chain: f.to_string(),
            to_chain: t.to_string(),
            collateral_chain: b.to_string(),
        }
        .data(),
        allways_swap_manager::accounts::OpenOrRequest {
            router: *router,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            quote: quote_pda(miner, f, t, b),
            attestation: (b != BACKING).then(|| attest_pda(miner, b)),
            pool: pool_pda(miner),
            treasury: treasury_pda(),
            reservation: resv_pda(miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn resolve_ix(caller: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::ResolvePool {}.data(),
        allways_swap_manager::accounts::ResolvePool {
            caller: *caller,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            pool: pool_pda(miner),
            reservation: resv_pda(miner),
            slot_hashes: SLOT_HASHES_ID,
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn finalize_ix(
    router: &Pubkey,
    miner: &Pubkey,
    collateral_amount: u64,
    from_amount: u128,
    to_amount: u128,
    attestation: Option<Pubkey>,
) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::FinalizeReservation {
            user: LOTTERY_USER,
            user_from_addr: "userSrcAddr".to_string(),
            user_to_addr: "userDstAddr".to_string(),
            collateral_amount,
            from_amount,
            to_amount,
        }
        .data(),
        allways_swap_manager::accounts::FinalizeReservation {
            router: *router,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner),
            attestation,
        }
        .to_account_metas(None),
    )
}
fn claim_ix(caller: &Pubkey, miner: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SubmitSwapClaim {
            swap_key: key,
            from_tx_hash: from_tx_hash.to_string(),
            from_tx_block: FROM_TX_BLOCK,
        }
        .data(),
        allways_swap_manager::accounts::SubmitSwapClaim {
            caller: *caller,
            config: config_pda(),
            miner: *miner,
            reservation: resv_pda(miner),
            swap: swap_pda(&key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn initiate_ix(
    validator: &Pubkey,
    miner: &Pubkey,
    from_tx_hash: &str,
    attestation: Option<Pubkey>,
) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteInitiate { swap_key: key }.data(),
        allways_swap_manager::accounts::VoteInitiate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner),
            vote_round: vote_pda(REQ_INITIATE, miner.as_ref()),
            swap: swap_pda(&key),
            attestation,
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn migrate_config_ix(admin: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::MigrateConfig {}.data(),
        allways_swap_manager::accounts::MigrateConfig {
            admin: *admin,
            config: config_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn migrate_miner_ix(admin: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::MigrateMinerState {}.data(),
        allways_swap_manager::accounts::MigrateMinerState {
            admin: *admin,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

fn config_acct(svm: &LiteSVM) -> Config {
    Config::try_deserialize(&mut svm.get_account(&config_pda()).unwrap().data.as_slice()).unwrap()
}
fn miner_state(svm: &LiteSVM, m: &Pubkey) -> MinerState {
    MinerState::try_deserialize(&mut svm.get_account(&miner_pda(m)).unwrap().data.as_slice()).unwrap()
}
fn attestation_acct(svm: &LiteSVM, m: &Pubkey, chain: &str) -> BondAttestation {
    BondAttestation::try_deserialize(&mut svm.get_account(&attest_pda(m, chain)).unwrap().data.as_slice())
        .unwrap()
}
fn reservation_acct(svm: &LiteSVM, m: &Pubkey) -> Reservation {
    Reservation::try_deserialize(&mut svm.get_account(&resv_pda(m)).unwrap().data.as_slice()).unwrap()
}
fn swap_acct(svm: &LiteSVM, key: &[u8; 32]) -> Swap {
    Swap::try_deserialize(&mut svm.get_account(&swap_pda(key)).unwrap().data.as_slice()).unwrap()
}

/// Rewrite an account in place, preserving its allocated length.
fn overwrite(svm: &mut LiteSVM, pda: Pubkey, serialized: Vec<u8>) {
    let old = svm.get_account(&pda).unwrap();
    let mut data = serialized;
    data.resize(old.data.len(), 0);
    svm.set_account(
        pda,
        Account { lamports: old.lamports, data, owner: old.owner, executable: old.executable, rent_epoch: old.rent_epoch },
    )
    .unwrap();
}
fn set_reservation_backing(svm: &mut LiteSVM, miner: &Pubkey, backing: &str) {
    let mut r = reservation_acct(svm, miner);
    r.collateral_chain = backing.to_string();
    let mut buf = Vec::new();
    r.try_serialize(&mut buf).unwrap();
    overwrite(svm, resv_pda(miner), buf);
}
fn set_swap_backing(svm: &mut LiteSVM, key: &[u8; 32], backing: &str) {
    let mut s = swap_acct(svm, key);
    s.collateral_chain = backing.to_string();
    let mut buf = Vec::new();
    s.try_serialize(&mut buf).unwrap();
    overwrite(svm, swap_pda(key), buf);
}
fn set_settling_until(svm: &mut LiteSVM, miner: &Pubkey, until: i64) {
    let mut ms = miner_state(svm, miner);
    ms.settling_until = until;
    let mut buf = Vec::new();
    ms.try_serialize(&mut buf).unwrap();
    overwrite(svm, miner_pda(miner), buf);
}
/// Age the heartbeat one second past the fuse. Done by backdating the field rather than by running the
/// clock forward, which would also expire the live reservation under test.
fn stale_heartbeat(svm: &mut LiteSVM) {
    let mut cfg = config_acct(svm);
    cfg.last_attest_heartbeat = now_ts(svm) - cfg.attest_max_age_secs - 1;
    let mut buf = Vec::new();
    cfg.try_serialize(&mut buf).unwrap();
    overwrite(svm, config_pda(), buf);
}

/// init + 3 validators + a miner holding COLLATERAL with quotes on both pairs, activated on "sol".
fn setup() -> (LiteSVM, Keypair, Vec<Keypair>, Keypair) {
    let mut svm = LiteSVM::new();
    svm.add_program(pid(), include_bytes!("../../../target/deploy/allways_swap_manager.so")).unwrap();
    set_clock(&mut svm, BASE_TS);

    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, init_ix(&admin.pubkey()), &admin.pubkey(), &admin).expect("init");

    let mut vals = Vec::new();
    for _ in 0..3 {
        let v = Keypair::new();
        svm.airdrop(&v.pubkey(), 100_000_000_000).unwrap();
        send(&mut svm, add_validator_ix(&admin.pubkey(), v.pubkey()), &admin.pubkey(), &admin).expect("add val");
        vals.push(v);
    }

    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, post_ix(&miner.pubkey(), COLLATERAL), &miner.pubkey(), &miner).expect("post");
    // Activate BEFORE quoting: W2b's set_quote refuses a quote whose backing the miner isn't serving.
    send(&mut svm, activate_ix(&vals[0].pubkey(), &miner.pubkey(), SOL, None), &vals[0].pubkey(), &vals[0]).expect("a0");
    send(&mut svm, activate_ix(&vals[1].pubkey(), &miner.pubkey(), SOL, None), &vals[1].pubkey(), &vals[1]).expect("a1");
    send(&mut svm, set_quote_ix(&miner.pubkey(), SPOKE_FROM, SPOKE_TO), &miner.pubkey(), &miner).expect("quote spoke");
    send(&mut svm, set_quote_ix(&miner.pubkey(), HUB_FROM, HUB_TO), &miner.pubkey(), &miner).expect("quote hub");

    (svm, admin, vals, miner)
}

/// Two validators beat the heartbeat round at the current clock.
fn beat_heartbeat(svm: &mut LiteSVM, vals: &[Keypair]) {
    send(svm, heartbeat_ix(&vals[0].pubkey()), &vals[0].pubkey(), &vals[0]).expect("hb0");
    send(svm, heartbeat_ix(&vals[1].pubkey()), &vals[1].pubkey(), &vals[1]).expect("hb1");
}

/// Two validators carry one attestation payload to quorum.
fn attest(svm: &mut LiteSVM, vals: &[Keypair], miner: &Pubkey, bal: u64, locked: bool, epoch: u64) {
    send(svm, attest_ix(&vals[0].pubkey(), miner, TAO, bal, locked, epoch), &vals[0].pubkey(), &vals[0])
        .expect("attest 0");
    send(svm, attest_ix(&vals[1].pubkey(), miner, TAO, bal, locked, epoch), &vals[1].pubkey(), &vals[1])
        .expect("attest 1");
}

fn arm_and_resolve(svm: &mut LiteSVM, val: &Keypair, miner: &Pubkey) {
    send(svm, resolve_ix(&val.pubkey(), miner), &val.pubkey(), val).expect("arm draw");
    let a = svm.get_account(&pool_pda(miner)).unwrap();
    let seed_slot = Pool::try_deserialize(&mut a.data.as_slice()).unwrap().seed_slot;
    let entries: Vec<(u64, Hash)> = [seed_slot - 1, seed_slot, seed_slot + 1]
        .iter()
        .map(|&s| (s, Hash::new_from_array([s as u8; 32])))
        .collect();
    svm.set_sysvar::<SlotHashes>(&SlotHashes::new(&entries));
    send(svm, resolve_ix(&val.pubkey(), miner), &val.pubkey(), val).expect("resolve");
}

fn draw(svm: &mut LiteSVM, val: &Keypair, miner: &Pubkey, f: &str, t: &str) {
    let now = now_ts(svm);
    send(svm, open_ix(&val.pubkey(), miner, f, t), &val.pubkey(), val).expect("open");
    set_clock(svm, now + POOL_WINDOW_SECS + 1);
    arm_and_resolve(svm, val, miner);
}

/// A drawn sol→tao reservation re-pinned to the TAO backing, ready to finalize.
fn draw_tao_backed(svm: &mut LiteSVM, val: &Keypair, miner: &Pubkey) {
    draw(svm, val, miner, HUB_FROM, HUB_TO);
    set_reservation_backing(svm, miner, TAO);
}

// --- the attestation round -------------------------------------------------------------------

#[test]
fn test_attestation_round_writes_the_bond_on_quorum() {
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();

    send(&mut svm, attest_ix(&vals[0].pubkey(), &m, TAO, 5_000_000_000, true, 3), &vals[0].pubkey(), &vals[0])
        .expect("first vote");
    assert_eq!(
        attestation_acct(&svm, &m, TAO).epoch,
        0,
        "one vote is not a quorum — nothing written yet"
    );

    send(&mut svm, attest_ix(&vals[1].pubkey(), &m, TAO, 5_000_000_000, true, 3), &vals[1].pubkey(), &vals[1])
        .expect("quorum");
    let a = attestation_acct(&svm, &m, TAO);
    assert_eq!(a.miner, m);
    assert_eq!(a.chain, TAO);
    assert_eq!(a.effective_balance, 5_000_000_000);
    assert!(a.locked);
    assert_eq!(a.epoch, 3);
    assert_eq!(a.attested_at, now_ts(&svm), "stamped from the chain's clock at quorum");
}

#[test]
fn test_attestation_round_binds_the_full_payload() {
    // The whole point of the payload hash: two validators reading different balances must CONFLICT,
    // not co-count into a quorum that writes whichever value arrived first.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();

    send(&mut svm, attest_ix(&vals[0].pubkey(), &m, TAO, 5_000_000_000, true, 3), &vals[0].pubkey(), &vals[0])
        .expect("first vote");
    for (bal, locked, epoch, what) in [
        (4_000_000_000u64, true, 3u64, "balance"),
        (5_000_000_000, false, 3, "lock bit"),
        (5_000_000_000, true, 4, "epoch"),
    ] {
        let err = send(&mut svm, attest_ix(&vals[1].pubkey(), &m, TAO, bal, locked, epoch), &vals[1].pubkey(), &vals[1])
            .expect_err("divergent payload must not co-count");
        assert!(err.contains("VoteHashMismatch"), "{what} not bound into the round: {err}");
    }
    assert_eq!(attestation_acct(&svm, &m, TAO).epoch, 0, "no quorum was ever reached");

    // The matching payload still closes the round.
    send(&mut svm, attest_ix(&vals[1].pubkey(), &m, TAO, 5_000_000_000, true, 3), &vals[1].pubkey(), &vals[1])
        .expect("matching payload reaches quorum");
    assert_eq!(attestation_acct(&svm, &m, TAO).effective_balance, 5_000_000_000);
}

#[test]
fn test_attestation_refuses_a_stale_epoch_but_allows_same_epoch_updates() {
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    attest(&mut svm, &vals, &m, 5_000_000_000, true, 5);

    // A round left over from before the vault bumped its epoch must never restore the old lock state.
    let err = send(&mut svm, attest_ix(&vals[0].pubkey(), &m, TAO, 9_000_000_000, true, 4), &vals[0].pubkey(), &vals[0])
        .expect_err("stale epoch");
    assert!(err.contains("AttestationEpochStale"), "{err}");
    assert_eq!(attestation_acct(&svm, &m, TAO).effective_balance, 5_000_000_000, "untouched");

    // Same-epoch balance moves are the normal case (fees, slashes, posts don't bump the vault epoch).
    attest(&mut svm, &vals, &m, 4_000_000_000, true, 5);
    assert_eq!(attestation_acct(&svm, &m, TAO).effective_balance, 4_000_000_000);
    // And a newer epoch (an unlock) applies.
    attest(&mut svm, &vals, &m, 0, false, 6);
    let a = attestation_acct(&svm, &m, TAO);
    assert!(!a.locked && a.epoch == 6);
}

#[test]
fn test_attestation_is_refused_for_a_locally_settled_or_unknown_backing() {
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let err = send(&mut svm, attest_ix(&vals[0].pubkey(), &m, SOL, 1, true, 1), &vals[0].pubkey(), &vals[0])
        .expect_err("sol needs no attestation");
    assert!(err.contains("BackingSettlesLocally"), "{err}");
    let err = send(&mut svm, attest_ix(&vals[0].pubkey(), &m, "btc", 1, true, 1), &vals[0].pubkey(), &vals[0])
        .expect_err("btc is not a hub");
    assert!(err.contains("BackingNotSupported"), "{err}");
}

#[test]
fn test_attestation_and_heartbeat_run_during_a_halt() {
    // Pinned invariant: relay instructions are exit-path. A halt that froze them would strand the
    // settlement of every in-flight TAO-backed swap.
    let (mut svm, admin, vals, miner) = setup();
    send(
        &mut svm,
        admin_ix(&admin.pubkey(), allways_swap_manager::instruction::SetHalted { halted: true }.data()),
        &admin.pubkey(),
        &admin,
    )
    .expect("halt");
    attest(&mut svm, &vals, &miner.pubkey(), 5_000_000_000, true, 1);
    beat_heartbeat(&mut svm, &vals);
    assert_eq!(config_acct(&svm).last_attest_heartbeat, now_ts(&svm));
}

// --- the global heartbeat + the fuse ---------------------------------------------------------

#[test]
fn test_heartbeat_round_advances_the_timestamp_on_quorum() {
    let (mut svm, _admin, vals, _miner) = setup();
    assert_eq!(config_acct(&svm).last_attest_heartbeat, 0, "fused closed on a fresh deploy");
    assert_eq!(config_acct(&svm).attest_max_age_secs, ATTEST_MAX_AGE_SECS);

    send(&mut svm, heartbeat_ix(&vals[0].pubkey()), &vals[0].pubkey(), &vals[0]).expect("hb0");
    assert_eq!(config_acct(&svm).last_attest_heartbeat, 0, "one vote is not a quorum");
    send(&mut svm, heartbeat_ix(&vals[1].pubkey()), &vals[1].pubkey(), &vals[1]).expect("hb1");
    let first = now_ts(&svm);
    assert_eq!(config_acct(&svm).last_attest_heartbeat, first);

    // The round is reusable — the next cadence beat lands in the same PDA.
    set_clock(&mut svm, first + 43_200);
    beat_heartbeat(&mut svm, &vals);
    assert_eq!(config_acct(&svm).last_attest_heartbeat, first + 43_200);
}

#[test]
fn test_tao_backed_entry_is_fused_by_heartbeat_age() {
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));
    beat_heartbeat(&mut svm, &vals);
    attest(&mut svm, &vals, &m, required_collateral(TAO_AMOUNT as u64), true, 1);

    // Fresh heartbeat: the TAO-backed fill goes through, sized against the TAO leg.
    draw_tao_backed(&mut svm, &vals[0], &m);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT, attest_key),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("fresh heartbeat ⇒ tao entry allowed");

    // Let the heartbeat age past the fuse and the next fill is refused.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));
    beat_heartbeat(&mut svm, &vals);
    attest(&mut svm, &vals, &m, required_collateral(TAO_AMOUNT as u64), true, 1);
    draw_tao_backed(&mut svm, &vals[0], &m);
    stale_heartbeat(&mut svm);
    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT, attest_key),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("stale heartbeat");
    assert!(err.contains("AttestationStale"), "{err}");
}

#[test]
fn test_the_fuse_is_rechecked_at_initiate() {
    // The heartbeat can go stale between the fill and the attestation, and initiate is the last gate
    // before the miner is obligated.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));
    beat_heartbeat(&mut svm, &vals);
    attest(&mut svm, &vals, &m, required_collateral(TAO_AMOUNT as u64), true, 1);
    draw_tao_backed(&mut svm, &vals[0], &m);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT, attest_key),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("finalize");
    send(&mut svm, claim_ix(&vals[0].pubkey(), &m, "srctx1"), &vals[0].pubkey(), &vals[0]).expect("claim");
    set_swap_backing(&mut svm, &swap_key("srctx1"), TAO);

    stale_heartbeat(&mut svm);
    let err = send(&mut svm, initiate_ix(&vals[0].pubkey(), &m, "srctx1", attest_key), &vals[0].pubkey(), &vals[0])
        .expect_err("stale heartbeat at the obligation gate");
    assert!(err.contains("AttestationStale"), "{err}");

    // Beat it again and the same attestation obligates the miner.
    beat_heartbeat(&mut svm, &vals);
    send(&mut svm, initiate_ix(&vals[0].pubkey(), &m, "srctx1", attest_key), &vals[0].pubkey(), &vals[0]).expect("i0");
    send(&mut svm, initiate_ix(&vals[1].pubkey(), &m, "srctx1", attest_key), &vals[1].pubkey(), &vals[1]).expect("i1");
    assert!(miner_state(&svm, &m).has_active_swap);
}

#[test]
fn test_tao_entry_needs_a_locked_attestation_with_enough_bond() {
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));
    let need = required_collateral(TAO_AMOUNT as u64);
    beat_heartbeat(&mut svm, &vals);

    // No attestation account passed at all.
    draw_tao_backed(&mut svm, &vals[0], &m);
    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT, None),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("no attestation");
    assert!(err.contains("AttestationMissing"), "{err}");

    // Unlocked bond backs nothing.
    attest(&mut svm, &vals, &m, need, false, 1);
    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT, attest_key),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("unlocked bond");
    assert!(err.contains("BondNotLocked"), "{err}");

    // Locked but a rao short of the 1.10× requirement — the untouched W1 guard, now reading rao.
    attest(&mut svm, &vals, &m, need - 1, true, 1);
    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT, attest_key),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("under-collateralized bond");
    assert!(err.contains("InsufficientCollateral"), "{err}");

    // Exactly 1.10× clears, and the local SOL vault was never consulted.
    attest(&mut svm, &vals, &m, need, true, 1);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT, attest_key),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("1.10× of the tao leg, in rao");
    assert_eq!(miner_state(&svm, &m).collateral, COLLATERAL, "sol purse untouched");
}

// --- busy-until-settled (the entry lock) ------------------------------------------------------

#[test]
fn test_non_sol_timeout_sets_both_locks_and_settling_blocks_entry() {
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));
    beat_heartbeat(&mut svm, &vals);
    attest(&mut svm, &vals, &m, required_collateral(TAO_AMOUNT as u64), true, 1);

    draw_tao_backed(&mut svm, &vals[0], &m);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT, attest_key),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("finalize");
    send(&mut svm, claim_ix(&vals[0].pubkey(), &m, "srctx1"), &vals[0].pubkey(), &vals[0]).expect("claim");
    set_swap_backing(&mut svm, &swap_key("srctx1"), TAO);
    let initiated_at = now_ts(&svm);
    send(&mut svm, initiate_ix(&vals[0].pubkey(), &m, "srctx1", attest_key), &vals[0].pubkey(), &vals[0]).expect("i0");
    send(&mut svm, initiate_ix(&vals[1].pubkey(), &m, "srctx1", attest_key), &vals[1].pubkey(), &vals[1]).expect("i1");

    let timeout_ts = initiated_at + TIMEOUT_SECS + 1;
    set_clock(&mut svm, timeout_ts);
    let key = swap_key("srctx1");
    let timeout = |v: &Keypair| {
        Instruction::new_with_bytes(
            pid(),
            &allways_swap_manager::instruction::TimeoutSwap { swap_key: key }.data(),
            allways_swap_manager::accounts::TimeoutSwap {
                validator: v.pubkey(),
                config: config_pda(),
                miner: m,
                miner_state: miner_pda(&m),
                collateral_vault: collateral_vault_pda(&m),
                user: LOTTERY_USER,
                swap: swap_pda(&key),
                vote_round: vote_pda(7, &key),
                system_program: SYSTEM_PROGRAM,
            }
            .to_account_metas(None),
        )
    };
    send(&mut svm, timeout(&vals[0]), &vals[0].pubkey(), &vals[0]).expect("t0");
    send(&mut svm, timeout(&vals[1]), &vals[1].pubkey(), &vals[1]).expect("t1");

    let ms = miner_state(&svm, &m);
    let settled_at = timeout_ts + SETTLEMENT_GRACE_SECS;
    assert_eq!(ms.busy_until, settled_at, "exit lock");
    assert_eq!(ms.settling_until, settled_at, "entry lock — a separate field, same deadline");

    // New work is refused while the penalty is still settling on the backing chain — at the first
    // door, the permissionless pool open, so no fresh contest can even be started.
    beat_heartbeat(&mut svm, &vals);
    let err = send(&mut svm, open_ix(&vals[0].pubkey(), &m, HUB_FROM, HUB_TO), &vals[0].pubkey(), &vals[0])
        .expect_err("still settling");
    assert!(err.contains("MinerSettling"), "{err}");

    // Past the grace it clears itself — no crank, no second write.
    set_clock(&mut svm, settled_at);
    beat_heartbeat(&mut svm, &vals);
    draw_tao_backed(&mut svm, &vals[0], &m);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT, attest_key),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("settled ⇒ entry reopens");
}

#[test]
fn test_a_pending_off_chain_settlement_freezes_the_sol_path_too() {
    // A pending seizure freezes the WHOLE miner: an outstanding TAO slash is money the miner owes that
    // nobody has taken yet, so it must not be able to open fresh SOL work in the meantime either.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let settled_at = BASE_TS + 100_000;

    // Draw first, so both SOL doors can be tried against the same outstanding settlement.
    draw(&mut svm, &vals[0], &m, SPOKE_FROM, SPOKE_TO);
    set_settling_until(&mut svm, &m, settled_at);

    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, SOL_AMOUNT, 1_333_333_333, SOL_AMOUNT as u128, None),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("sol-backed finalize while a TAO seizure is outstanding");
    assert!(err.contains("MinerSettling"), "{err}");

    // And the user-triggered door, which has no validator in the loop at all.
    let err = send(&mut svm, open_ix(&vals[1].pubkey(), &m, SPOKE_FROM, SPOKE_TO), &vals[1].pubkey(), &vals[1])
        .expect_err("sol-backed pool open while a TAO seizure is outstanding");
    assert!(err.contains("MinerSettling"), "{err}");

    // Self-clearing, like the TAO side: past the grace the same fill goes through, no crank.
    set_clock(&mut svm, settled_at);
    draw(&mut svm, &vals[0], &m, SPOKE_FROM, SPOKE_TO);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, SOL_AMOUNT, 1_333_333_333, SOL_AMOUNT as u128, None),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("settled ⇒ SOL entry reopens");
}

#[test]
fn test_a_sol_only_miner_is_untouched_by_the_settle_gate() {
    // The gate costs the SOL path nothing in the normal case: `settling_until` is 0 for a SOL-only
    // miner and a locally-settled timeout never writes it, so the whole SOL lifecycle is unaffected.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    assert_eq!(miner_state(&svm, &m).settling_until, 0, "nothing has ever settled off-chain here");

    draw(&mut svm, &vals[0], &m, SPOKE_FROM, SPOKE_TO);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, SOL_AMOUNT, 1_333_333_333, SOL_AMOUNT as u128, None),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("finalize");
    send(&mut svm, claim_ix(&vals[0].pubkey(), &m, "soltx2"), &vals[0].pubkey(), &vals[0]).expect("claim");
    let initiated_at = now_ts(&svm);
    send(&mut svm, initiate_ix(&vals[0].pubkey(), &m, "soltx2", None), &vals[0].pubkey(), &vals[0]).expect("i0");
    send(&mut svm, initiate_ix(&vals[1].pubkey(), &m, "soltx2", None), &vals[1].pubkey(), &vals[1]).expect("i1");

    // And the local timeout that follows leaves it at 0 — the seizure already happened, atomically.
    set_clock(&mut svm, initiated_at + TIMEOUT_SECS + 1);
    let key = swap_key("soltx2");
    send(&mut svm, timeout_ix(&vals[0].pubkey(), &m, key), &vals[0].pubkey(), &vals[0]).expect("t0");
    send(&mut svm, timeout_ix(&vals[1].pubkey(), &m, key), &vals[1].pubkey(), &vals[1]).expect("t1");
    assert_eq!(miner_state(&svm, &m).settling_until, 0, "a local seizure owes nothing later");
}

// --- per-hub activation ------------------------------------------------------------------------

#[test]
fn test_activate_tao_gate_matrix() {
    let (mut svm, admin, vals, miner) = setup();
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));

    // Stale heartbeat: the fuse gates activation too — a bond nobody is refreshing is not a bond. The
    // fuse fires before the purse is even read, so no attestation account is needed to reach it.
    let err = send(&mut svm, activate_ix(&vals[0].pubkey(), &m, TAO, None), &vals[0].pubkey(), &vals[0])
        .expect_err("stale heartbeat");
    assert!(err.contains("AttestationStale"), "{err}");

    beat_heartbeat(&mut svm, &vals);
    let err = send(&mut svm, activate_ix(&vals[0].pubkey(), &m, TAO, None), &vals[0].pubkey(), &vals[0])
        .expect_err("no attestation");
    assert!(err.contains("AttestationMissing"), "{err}");

    attest(&mut svm, &vals, &m, TAO_MIN_COLLATERAL_RAO, false, 1);
    let err = send(&mut svm, activate_ix(&vals[0].pubkey(), &m, TAO, attest_key), &vals[0].pubkey(), &vals[0])
        .expect_err("unlocked bond");
    assert!(err.contains("BondNotLocked"), "{err}");

    attest(&mut svm, &vals, &m, TAO_MIN_COLLATERAL_RAO - 1, true, 1);
    let err = send(&mut svm, activate_ix(&vals[0].pubkey(), &m, TAO, attest_key), &vals[0].pubkey(), &vals[0])
        .expect_err("below the rao floor");
    assert!(err.contains("InsufficientCollateral"), "{err}");

    // The rao floor is its own knob — raising it re-closes the gate on a bond that just cleared it.
    attest(&mut svm, &vals, &m, TAO_MIN_COLLATERAL_RAO, true, 1);
    send(
        &mut svm,
        admin_ix(
            &admin.pubkey(),
            allways_swap_manager::instruction::SetTaoMinCollateral { amount: TAO_MIN_COLLATERAL_RAO + 1 }.data(),
        ),
        &admin.pubkey(),
        &admin,
    )
    .expect("raise the tao floor");
    let err = send(&mut svm, activate_ix(&vals[0].pubkey(), &m, TAO, attest_key), &vals[0].pubkey(), &vals[0])
        .expect_err("floor raised above the bond");
    assert!(err.contains("InsufficientCollateral"), "{err}");
    assert_eq!(config_acct(&svm).min_collateral, MIN_COLLATERAL, "the lamport floor is a separate knob");

    // Happy path: lower it back and two votes light the TAO bit.
    send(
        &mut svm,
        admin_ix(
            &admin.pubkey(),
            allways_swap_manager::instruction::SetTaoMinCollateral { amount: TAO_MIN_COLLATERAL_RAO }.data(),
        ),
        &admin.pubkey(),
        &admin,
    )
    .expect("restore the tao floor");
    send(&mut svm, activate_ix(&vals[0].pubkey(), &m, TAO, attest_key), &vals[0].pubkey(), &vals[0]).expect("t0");
    send(&mut svm, activate_ix(&vals[1].pubkey(), &m, TAO, attest_key), &vals[1].pubkey(), &vals[1]).expect("t1");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, BACKING_BIT_SOL | BACKING_BIT_TAO);
    assert!(ms.active);
}

#[test]
fn test_activate_rounds_bind_the_backing() {
    // Without the backing in the hash a vote for "sol" would count toward activating "tao".
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));
    beat_heartbeat(&mut svm, &vals);
    attest(&mut svm, &vals, &m, TAO_MIN_COLLATERAL_RAO, true, 1);

    send(&mut svm, activate_ix(&vals[0].pubkey(), &m, TAO, attest_key), &vals[0].pubkey(), &vals[0]).expect("t0");
    let err = send(&mut svm, activate_ix(&vals[1].pubkey(), &m, SOL, None), &vals[1].pubkey(), &vals[1])
        .expect_err("sol is already active, and would be a different round anyway");
    assert!(err.contains("MinerAlreadyActive"), "{err}");
    assert_eq!(miner_state(&svm, &m).active_backings, BACKING_BIT_SOL, "tao still needs its second vote");

    // Deactivating sol frees the guard; the leftover tao round must still refuse a sol vote.
    send(&mut svm, deactivate_ix(&vals[0].pubkey(), &m, SOL), &vals[0].pubkey(), &vals[0]).expect("d0");
    send(&mut svm, deactivate_ix(&vals[1].pubkey(), &m, SOL), &vals[1].pubkey(), &vals[1]).expect("d1");
    let err = send(&mut svm, activate_ix(&vals[1].pubkey(), &m, SOL, None), &vals[1].pubkey(), &vals[1])
        .expect_err("a sol vote cannot join the open tao round");
    assert!(err.contains("VoteHashMismatch"), "{err}");
}

#[test]
fn test_bitmask_transitions_keep_the_or_view_exact() {
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));
    beat_heartbeat(&mut svm, &vals);
    attest(&mut svm, &vals, &m, TAO_MIN_COLLATERAL_RAO, true, 1);
    send(&mut svm, activate_ix(&vals[0].pubkey(), &m, TAO, attest_key), &vals[0].pubkey(), &vals[0]).expect("t0");
    send(&mut svm, activate_ix(&vals[1].pubkey(), &m, TAO, attest_key), &vals[1].pubkey(), &vals[1]).expect("t1");
    assert_eq!(miner_state(&svm, &m).active_backings, BACKING_BIT_SOL | BACKING_BIT_TAO);

    // Sweeping the TAO purse leaves SOL trading — and does NOT start the withdrawal cooldown.
    send(&mut svm, deactivate_ix(&vals[0].pubkey(), &m, TAO), &vals[0].pubkey(), &vals[0]).expect("d0");
    send(&mut svm, deactivate_ix(&vals[1].pubkey(), &m, TAO), &vals[1].pubkey(), &vals[1]).expect("d1");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, BACKING_BIT_SOL);
    assert!(ms.active, "the OR view is still lit");
    assert_eq!(ms.deactivation_at, 0, "the miner has not left");

    // Dropping the last purse is what actually deactivates the miner.
    send(&mut svm, deactivate_ix(&vals[0].pubkey(), &m, SOL), &vals[0].pubkey(), &vals[0]).expect("d2");
    send(&mut svm, deactivate_ix(&vals[1].pubkey(), &m, SOL), &vals[1].pubkey(), &vals[1]).expect("d3");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, 0);
    assert!(!ms.active);
    assert_eq!(ms.deactivation_at, now_ts(&svm));

    // An already-dark backing can't be swept again.
    let err = send(&mut svm, deactivate_ix(&vals[0].pubkey(), &m, TAO), &vals[0].pubkey(), &vals[0])
        .expect_err("bit already clear");
    assert!(err.contains("MinerNotActive"), "{err}");
}

#[test]
fn test_self_deactivate_drops_every_backing() {
    // The exit door, not a per-purse sweep: `active` must never survive as true over a cleared mask.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));
    beat_heartbeat(&mut svm, &vals);
    attest(&mut svm, &vals, &m, TAO_MIN_COLLATERAL_RAO, true, 1);
    send(&mut svm, activate_ix(&vals[0].pubkey(), &m, TAO, attest_key), &vals[0].pubkey(), &vals[0]).expect("t0");
    send(&mut svm, activate_ix(&vals[1].pubkey(), &m, TAO, attest_key), &vals[1].pubkey(), &vals[1]).expect("t1");

    let ix = Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Deactivate { backing: None }.data(),
        allways_swap_manager::accounts::Deactivate { miner: m, miner_state: miner_pda(&m) }
            .to_account_metas(None),
    );
    send(&mut svm, ix, &m, &miner).expect("self-deactivate");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, 0);
    assert!(!ms.active);
}

// --- v3 upgrade cranks ---------------------------------------------------------------------------

/// The v10 `Config` body, as the deployed program wrote it (no W1/W2 fields).
#[derive(AnchorSerialize)]
struct ConfigV10 {
    admin: Pubkey,
    version: u32,
    min_collateral: u64,
    max_collateral: u64,
    fulfillment_timeout_secs: i64,
    min_swap_amount: u64,
    max_swap_amount: u64,
    reservation_ttl_secs: i64,
    consensus_threshold_percent: u8,
    validators: Vec<ValidatorInfo>,
    last_weights_update: i64,
    halted: bool,
    reservation_fee_lamports: u64,
    pool_window_secs: i64,
    finalize_window_secs: i64,
    weights_update_min_interval_secs: i64,
    max_total_extension_secs: i64,
    bump: u8,
}

/// The v10 `MinerState` body (no `active_backings`, no `settling_until`).
#[derive(AnchorSerialize)]
struct MinerStateV10 {
    miner: Pubkey,
    collateral: u64,
    active: bool,
    has_active_swap: bool,
    busy_until: i64,
    deactivation_at: i64,
    successful_swaps: u32,
    failed_swaps: u32,
    bump: u8,
}

/// Replace an account with a shorter, legacy-layout body — the exact shape the v3 upgrade inherits.
fn downgrade(svm: &mut LiteSVM, pda: Pubkey, disc: &[u8], body: Vec<u8>) {
    let old = svm.get_account(&pda).unwrap();
    let mut data = disc.to_vec();
    data.extend(body);
    let lamports = svm.minimum_balance_for_rent_exemption(data.len());
    svm.set_account(
        pda,
        Account { lamports, data, owner: old.owner, executable: old.executable, rent_epoch: old.rent_epoch },
    )
    .unwrap();
}

#[test]
fn test_migrate_config_seeds_the_new_fields_and_is_idempotent() {
    let (mut svm, admin, vals, _miner) = setup();
    let live = config_acct(&svm);
    let legacy = ConfigV10 {
        admin: admin.pubkey(),
        version: 10,
        min_collateral: live.min_collateral,
        max_collateral: live.max_collateral,
        fulfillment_timeout_secs: live.fulfillment_timeout_secs,
        min_swap_amount: live.min_swap_amount,
        max_swap_amount: live.max_swap_amount,
        reservation_ttl_secs: live.reservation_ttl_secs,
        consensus_threshold_percent: live.consensus_threshold_percent,
        validators: live.validators.clone(),
        last_weights_update: 123,
        halted: true,
        reservation_fee_lamports: live.reservation_fee_lamports,
        pool_window_secs: live.pool_window_secs,
        finalize_window_secs: live.finalize_window_secs,
        weights_update_min_interval_secs: live.weights_update_min_interval_secs,
        max_total_extension_secs: live.max_total_extension_secs,
        bump: live.bump,
    };
    let mut body = Vec::new();
    legacy.serialize(&mut body).unwrap();
    let short_len = 8 + body.len();
    downgrade(&mut svm, config_pda(), &Config::DISCRIMINATOR, body);
    assert!(svm.get_account(&config_pda()).unwrap().data.len() < 8 + Config::INIT_SPACE);

    send(&mut svm, migrate_config_ix(&admin.pubkey()), &admin.pubkey(), &admin).expect("migrate");
    let cfg = config_acct(&svm);
    assert_eq!(cfg.version, CONFIG_VERSION);
    // Everything the v10 program owned is carried across verbatim...
    assert_eq!(cfg.admin, admin.pubkey());
    assert_eq!(cfg.validators.len(), vals.len());
    assert_eq!(cfg.last_weights_update, 123);
    assert!(cfg.halted, "a halted subnet stays halted across the upgrade");
    assert_eq!(cfg.max_total_extension_secs, live.max_total_extension_secs);
    // ...and the W1+W2 fields arrive at their seeded defaults, fuse closed.
    assert_eq!(cfg.tao_min_collateral, TAO_MIN_COLLATERAL_RAO);
    assert_eq!(cfg.settlement_grace_secs, SETTLEMENT_GRACE_SECS);
    assert_eq!(cfg.attest_max_age_secs, ATTEST_MAX_AGE_SECS);
    assert_eq!(cfg.last_attest_heartbeat, 0);
    assert!(svm.get_account(&config_pda()).unwrap().data.len() > short_len, "grown");

    // Re-running is a no-op, not a re-seed: the second call must not stomp post-migration state.
    send(
        &mut svm,
        admin_ix(&admin.pubkey(), allways_swap_manager::instruction::SetTaoMinCollateral { amount: 777 }.data()),
        &admin.pubkey(),
        &admin,
    )
    .expect("set floor");
    send(&mut svm, migrate_config_ix(&admin.pubkey()), &admin.pubkey(), &admin).expect("migrate again");
    assert_eq!(config_acct(&svm).tao_min_collateral, 777, "idempotent, not re-seeding");
    assert_eq!(config_acct(&svm).version, CONFIG_VERSION);
}

#[test]
fn test_migrate_miner_state_carries_active_into_the_sol_bit_and_is_idempotent() {
    let (mut svm, admin, _vals, miner) = setup();
    let m = miner.pubkey();
    let live = miner_state(&svm, &m);
    let legacy = MinerStateV10 {
        miner: m,
        collateral: live.collateral,
        active: true,
        has_active_swap: false,
        busy_until: 42,
        deactivation_at: 7,
        successful_swaps: 5,
        failed_swaps: 2,
        bump: live.bump,
    };
    let mut body = Vec::new();
    legacy.serialize(&mut body).unwrap();
    downgrade(&mut svm, miner_pda(&m), &MinerState::DISCRIMINATOR, body);

    send(&mut svm, migrate_miner_ix(&admin.pubkey(), &m), &admin.pubkey(), &admin).expect("migrate");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, BACKING_BIT_SOL, "the legacy bool WAS the sol purse");
    assert!(ms.active, "OR view unchanged — no re-activation vote needed at the upgrade");
    assert_eq!(ms.settling_until, 0);
    assert_eq!(ms.collateral, live.collateral);
    assert_eq!(ms.busy_until, 42);
    assert_eq!(ms.deactivation_at, 7);
    assert_eq!(ms.successful_swaps, 5);
    assert_eq!(ms.failed_swaps, 2);

    // Idempotent: a second crank must not wipe the bits the first one seeded.
    send(&mut svm, migrate_miner_ix(&admin.pubkey(), &m), &admin.pubkey(), &admin).expect("migrate again");
    assert_eq!(miner_state(&svm, &m).active_backings, BACKING_BIT_SOL);
}

#[test]
fn test_migrate_miner_state_seeds_an_inactive_miner_with_no_bits() {
    let (mut svm, admin, _vals, miner) = setup();
    let m = miner.pubkey();
    let live = miner_state(&svm, &m);
    let legacy = MinerStateV10 {
        miner: m,
        collateral: live.collateral,
        active: false,
        has_active_swap: false,
        busy_until: 0,
        deactivation_at: 0,
        successful_swaps: 0,
        failed_swaps: 0,
        bump: live.bump,
    };
    let mut body = Vec::new();
    legacy.serialize(&mut body).unwrap();
    downgrade(&mut svm, miner_pda(&m), &MinerState::DISCRIMINATOR, body);

    send(&mut svm, migrate_miner_ix(&admin.pubkey(), &m), &admin.pubkey(), &admin).expect("migrate");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, 0);
    assert!(!ms.active);
}

#[test]
fn test_migration_cranks_are_admin_only() {
    let (mut svm, admin, _vals, miner) = setup();
    let intruder = Keypair::new();
    svm.airdrop(&intruder.pubkey(), 10_000_000_000).unwrap();

    // migrate_miner_state is gated by the live Config's `has_one = admin`.
    let err = send(
        &mut svm,
        migrate_miner_ix(&intruder.pubkey(), &miner.pubkey()),
        &intruder.pubkey(),
        &intruder,
    )
    .expect_err("not admin");
    assert!(err.contains("ConstraintHasOne") || err.contains("2001"), "{err}");

    // migrate_config compares against the admin in the account's raw bytes (it can't deserialize a
    // legacy Config to use `has_one`).
    let live = config_acct(&svm);
    let legacy = ConfigV10 {
        admin: admin.pubkey(),
        version: 10,
        min_collateral: live.min_collateral,
        max_collateral: live.max_collateral,
        fulfillment_timeout_secs: live.fulfillment_timeout_secs,
        min_swap_amount: live.min_swap_amount,
        max_swap_amount: live.max_swap_amount,
        reservation_ttl_secs: live.reservation_ttl_secs,
        consensus_threshold_percent: live.consensus_threshold_percent,
        validators: live.validators.clone(),
        last_weights_update: 0,
        halted: false,
        reservation_fee_lamports: live.reservation_fee_lamports,
        pool_window_secs: live.pool_window_secs,
        finalize_window_secs: live.finalize_window_secs,
        weights_update_min_interval_secs: live.weights_update_min_interval_secs,
        max_total_extension_secs: live.max_total_extension_secs,
        bump: live.bump,
    };
    let mut body = Vec::new();
    legacy.serialize(&mut body).unwrap();
    downgrade(&mut svm, config_pda(), &Config::DISCRIMINATOR, body);
    assert!(send(&mut svm, migrate_config_ix(&intruder.pubkey()), &intruder.pubkey(), &intruder).is_err());
    send(&mut svm, migrate_config_ix(&admin.pubkey()), &admin.pubkey(), &admin).expect("admin may");
}

// --- W2b: quote-declared backing reaches the pool and the draw -------------------------------------

/// Light the miner's TAO purse (heartbeat + attestation + activation quorum) and post a TAO-backed
/// quote on the hub↔hub pair. `bond` is the attested effective balance.
fn light_tao_purse(svm: &mut LiteSVM, vals: &[Keypair], miner: &Keypair, bond: u64) {
    let m = miner.pubkey();
    let attest_key = Some(attest_pda(&m, TAO));
    beat_heartbeat(svm, vals);
    attest(svm, vals, &m, bond, true, 1);
    send(svm, activate_ix(&vals[0].pubkey(), &m, TAO, attest_key), &vals[0].pubkey(), &vals[0]).expect("t0");
    send(svm, activate_ix(&vals[1].pubkey(), &m, TAO, attest_key), &vals[1].pubkey(), &vals[1]).expect("t1");
    send(svm, set_quote_backed_ix(&m, HUB_FROM, HUB_TO, TAO), &m, miner).expect("tao-backed quote");
}

fn self_deactivate_ix(miner: &Pubkey, backing: Option<&str>) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Deactivate { backing: backing.map(|b| b.to_string()) }
            .data(),
        allways_swap_manager::accounts::Deactivate { miner: *miner, miner_state: miner_pda(miner) }
            .to_account_metas(None),
    )
}

#[test]
fn test_tao_backed_pool_entry_reads_the_tao_purse_not_the_local_vault() {
    // The W2 flag closed: open_or_request used to floor on lamports unconditionally. A TAO-backed
    // quote must be gated on the ATTESTED bond, and a bond under the TAO floor must not open a pool
    // however much SOL the miner happens to hold.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    light_tao_purse(&mut svm, &vals, &miner, TAO_MIN_COLLATERAL_RAO);

    // A slash lands: the attested bond drops under the TAO floor while the miner's SOL vault is
    // untouched. The bid must die on the TAO purse, not pass on the lamports it still holds.
    attest(&mut svm, &vals, &m, TAO_MIN_COLLATERAL_RAO - 1, true, 2);
    let err = send(&mut svm, open_backed_ix(&vals[0].pubkey(), &m, HUB_FROM, HUB_TO, TAO), &vals[0].pubkey(), &vals[0])
        .unwrap_err();
    assert!(
        err.contains("InsufficientCollateral"),
        "a bond below tao_min_collateral must not open a pool despite {COLLATERAL} lamports of SOL, got: {err}"
    );
    assert_eq!(miner_state(&svm, &m).collateral, COLLATERAL, "the local vault was never consulted");

    // Bond restored to the floor and the same bid goes through.
    attest(&mut svm, &vals, &m, TAO_MIN_COLLATERAL_RAO, true, 3);
    send(&mut svm, open_backed_ix(&vals[0].pubkey(), &m, HUB_FROM, HUB_TO, TAO), &vals[0].pubkey(), &vals[0])
        .expect("tao-backed open at the floor");
}

#[test]
fn test_tao_backed_pool_entry_needs_a_locked_bond_and_a_live_fuse() {
    // Each gate on its own, all of them reached only because the quote declared "tao".
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    light_tao_purse(&mut svm, &vals, &miner, TAO_AMOUNT as u64);

    // Unlocked bond: the vault no longer holds it for us.
    attest(&mut svm, &vals, &m, TAO_AMOUNT as u64, false, 2);
    let err = send(&mut svm, open_backed_ix(&vals[0].pubkey(), &m, HUB_FROM, HUB_TO, TAO), &vals[0].pubkey(), &vals[0])
        .unwrap_err();
    assert!(err.contains("BondNotLocked"), "unlocked bond must not open a pool, got: {err}");

    // Re-locked, but the heartbeat has gone stale — the dead-man fuse shuts entry.
    attest(&mut svm, &vals, &m, TAO_AMOUNT as u64, true, 3);
    let stale_at = now_ts(&svm) + config_acct(&svm).attest_max_age_secs + 1;
    set_clock(&mut svm, stale_at);
    let err = send(&mut svm, open_backed_ix(&vals[0].pubkey(), &m, HUB_FROM, HUB_TO, TAO), &vals[0].pubkey(), &vals[0])
        .unwrap_err();
    assert!(err.contains("AttestationStale"), "a stale heartbeat must fuse TAO entry off, got: {err}");

    // Heartbeat restored → open succeeds.
    beat_heartbeat(&mut svm, &vals);
    send(&mut svm, open_backed_ix(&vals[0].pubkey(), &m, HUB_FROM, HUB_TO, TAO), &vals[0].pubkey(), &vals[0])
        .expect("open once the fuse is live again");
}

#[test]
fn test_a_tao_only_miner_serves_tao_quotes_and_nothing_else() {
    // D2's point: purses are independent. A miner with no SOL bit cannot open a SOL-backed pool even
    // on a pair it quotes, and its TAO-backed pool opens fine.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    light_tao_purse(&mut svm, &vals, &miner, TAO_AMOUNT as u64);
    // Drop the SOL purse, leaving the (already posted) sol-backed quotes standing behind a dark purse.
    send(&mut svm, self_deactivate_ix(&m, Some(SOL)), &m, &miner).expect("drop sol purse");

    let err = send(&mut svm, open_ix(&vals[0].pubkey(), &m, SPOKE_FROM, SPOKE_TO), &vals[0].pubkey(), &vals[0])
        .unwrap_err();
    assert!(err.contains("MinerNotActive"), "sol-backed pool with a dark SOL purse, got: {err}");

    send(&mut svm, open_backed_ix(&vals[0].pubkey(), &m, HUB_FROM, HUB_TO, TAO), &vals[0].pubkey(), &vals[0])
        .expect("a TAO-only miner still serves its TAO-backed quote");
}

#[test]
fn test_resolve_pool_pins_the_drawn_quotes_backing() {
    // The W1 pin point, now fed by the quote instead of the "sol" constant — reached entirely through
    // the public path (no hand-written reservation), which is what makes TAO swaps reachable at all.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    light_tao_purse(&mut svm, &vals, &miner, TAO_AMOUNT as u64);

    let now = now_ts(&svm);
    send(&mut svm, open_backed_ix(&vals[0].pubkey(), &m, HUB_FROM, HUB_TO, TAO), &vals[0].pubkey(), &vals[0])
        .expect("open");
    assert_eq!(
        Pool::try_deserialize(&mut svm.get_account(&pool_pda(&m)).unwrap().data.as_slice()).unwrap().collateral_chain,
        TAO,
        "the pool pins the quote's backing at open"
    );
    set_clock(&mut svm, now + POOL_WINDOW_SECS + 1);
    arm_and_resolve(&mut svm, &vals[0], &m);

    let r = reservation_acct(&svm, &m);
    assert_eq!(r.collateral_chain, TAO, "the draw copies the pool's backing into the Reservation");
    assert_eq!(r.from_chain, HUB_FROM);
    assert_eq!(r.to_chain, HUB_TO);
}

#[test]
fn test_a_bid_cannot_join_a_pool_pinned_to_a_different_backing() {
    // Two quotes, same direction, different guarantee — a late bidder must not be able to slide onto
    // the other offer's contest.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    light_tao_purse(&mut svm, &vals, &miner, TAO_AMOUNT as u64);

    send(&mut svm, open_backed_ix(&vals[0].pubkey(), &m, HUB_FROM, HUB_TO, TAO), &vals[0].pubkey(), &vals[0])
        .expect("open tao-backed");
    let err = send(&mut svm, open_backed_ix(&vals[1].pubkey(), &m, HUB_FROM, HUB_TO, SOL), &vals[1].pubkey(), &vals[1])
        .unwrap_err();
    assert!(err.contains("MinerBusyDifferentPair"), "backing mismatch must be refused, got: {err}");
}

#[test]
fn test_partial_self_exit_drops_one_purse_and_only_sol_starts_the_cooldown() {
    // The cooldown guards LOCAL collateral: dropping TAO leaves it unset (that exit is gated on the
    // vault's unlock path), dropping SOL starts it.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    light_tao_purse(&mut svm, &vals, &miner, TAO_AMOUNT as u64);
    assert_eq!(miner_state(&svm, &m).active_backings, BACKING_BIT_SOL | BACKING_BIT_TAO);

    send(&mut svm, self_deactivate_ix(&m, Some(TAO)), &m, &miner).expect("tao exit");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, BACKING_BIT_SOL, "only the TAO bit dropped");
    assert!(ms.active, "the miner still trades on SOL");
    assert_eq!(ms.deactivation_at, 0, "a TAO exit must not start the local-collateral cooldown");

    let at = now_ts(&svm);
    send(&mut svm, self_deactivate_ix(&m, Some(SOL)), &m, &miner).expect("sol exit");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, 0);
    assert!(!ms.active);
    assert_eq!(ms.deactivation_at, at, "dropping the SOL purse starts the cooldown");
}

#[test]
fn test_partial_self_exit_of_a_dark_purse_is_refused() {
    // Idempotence would silently re-stamp `deactivation_at` and slide the cooldown forward.
    let (mut svm, _admin, _vals, miner) = setup();
    let m = miner.pubkey();
    let err = send(&mut svm, self_deactivate_ix(&m, Some(TAO)), &m, &miner).unwrap_err();
    assert!(err.contains("MinerNotActive"), "dropping an already-dark purse, got: {err}");
}

// --- the withdrawal cooldown tracks the SOL bit in EVERY writer -----------------------------------

fn timeout_ix(validator: &Pubkey, miner: &Pubkey, key: [u8; 32]) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::TimeoutSwap { swap_key: key }.data(),
        allways_swap_manager::accounts::TimeoutSwap {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            collateral_vault: collateral_vault_pda(miner),
            user: LOTTERY_USER,
            swap: swap_pda(&key),
            vote_round: vote_pda(REQ_TIMEOUT, &key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

#[test]
fn test_a_slash_that_drops_only_the_sol_bit_still_starts_the_cooldown() {
    // `apply_penalty` used to stamp `deactivation_at` only when the LAST purse went dark, so a slashed
    // dual-purse miner kept a zero stamp — and `withdraw_collateral` skips the cooldown entirely at
    // zero, so dropping the (already dark) TAO purse afterwards freed the collateral immediately.
    let (mut svm, admin, vals, miner) = setup();
    let m = miner.pubkey();
    light_tao_purse(&mut svm, &vals, &miner, TAO_AMOUNT as u64);

    // A SOL-backed swap on the spoke pair, driven to an in-flight state through the public path.
    draw(&mut svm, &vals[0], &m, SPOKE_FROM, SPOKE_TO);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &m, SOL_AMOUNT, 1_333_333_333, SOL_AMOUNT as u128, None),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("finalize");
    send(&mut svm, claim_ix(&vals[0].pubkey(), &m, "soltx1"), &vals[0].pubkey(), &vals[0]).expect("claim");
    let initiated_at = now_ts(&svm);
    send(&mut svm, initiate_ix(&vals[0].pubkey(), &m, "soltx1", None), &vals[0].pubkey(), &vals[0]).expect("i0");
    send(&mut svm, initiate_ix(&vals[1].pubkey(), &m, "soltx1", None), &vals[1].pubkey(), &vals[1]).expect("i1");

    // Raise the floor so the 1.1× seizure leaves the SOL purse deficient — the branch that drops the bit.
    send(
        &mut svm,
        admin_ix(
            &admin.pubkey(),
            allways_swap_manager::instruction::SetMinCollateral { amount: COLLATERAL - 1 }.data(),
        ),
        &admin.pubkey(),
        &admin,
    )
    .expect("raise the sol floor");

    let timeout_ts = initiated_at + TIMEOUT_SECS + 1;
    set_clock(&mut svm, timeout_ts);
    let key = swap_key("soltx1");
    send(&mut svm, timeout_ix(&vals[0].pubkey(), &m, key), &vals[0].pubkey(), &vals[0]).expect("t0");
    send(&mut svm, timeout_ix(&vals[1].pubkey(), &m, key), &vals[1].pubkey(), &vals[1]).expect("t1");

    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, BACKING_BIT_TAO, "the deficient SOL purse went dark, TAO kept trading");
    assert!(ms.active, "the miner has not left — the OR view still holds");
    assert_eq!(ms.deactivation_at, timeout_ts, "the SOL bit dropping starts the local-collateral cooldown");
}

#[test]
fn test_lighting_another_purse_does_not_clear_a_running_sol_cooldown() {
    // `vote_activate` used to zero `deactivation_at` on ANY backing's activation, so a miner inside its
    // SOL withdrawal cooldown could wipe the clock by lighting TAO instead of waiting it out.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();

    let dropped_at = now_ts(&svm);
    send(&mut svm, self_deactivate_ix(&m, Some(SOL)), &m, &miner).expect("drop the sol purse");
    assert_eq!(miner_state(&svm, &m).deactivation_at, dropped_at, "cooldown running");

    light_tao_purse(&mut svm, &vals, &miner, TAO_AMOUNT as u64);
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, BACKING_BIT_TAO, "TAO lit, SOL still dark");
    assert_eq!(ms.deactivation_at, dropped_at, "a TAO activation must not clear the SOL cooldown");

    // Only the SOL purse coming back clears the cooldown it started.
    send(&mut svm, activate_ix(&vals[0].pubkey(), &m, SOL, None), &vals[0].pubkey(), &vals[0]).expect("s0");
    send(&mut svm, activate_ix(&vals[1].pubkey(), &m, SOL, None), &vals[1].pubkey(), &vals[1]).expect("s1");
    assert_eq!(miner_state(&svm, &m).deactivation_at, 0, "re-lighting SOL clears its own cooldown");
}

#[test]
fn test_a_forced_sol_deactivation_starts_the_cooldown_on_a_still_lit_miner() {
    // The vote_deactivate half of the same rule (the #616 floor sweep, per purse): the cooldown keys off
    // the SOL bit, not off the miner going fully dark.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    light_tao_purse(&mut svm, &vals, &miner, TAO_AMOUNT as u64);

    let swept_at = now_ts(&svm);
    send(&mut svm, deactivate_ix(&vals[0].pubkey(), &m, SOL), &vals[0].pubkey(), &vals[0]).expect("d0");
    send(&mut svm, deactivate_ix(&vals[1].pubkey(), &m, SOL), &vals[1].pubkey(), &vals[1]).expect("d1");

    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, BACKING_BIT_TAO, "only the swept purse went dark");
    assert!(ms.active);
    assert_eq!(ms.deactivation_at, swept_at, "a forced SOL sweep starts the cooldown too");
}

#[test]
fn test_a_forced_tao_deactivation_leaves_the_sol_cooldown_alone() {
    // The other side of it: sweeping TAO must not stamp the local-collateral clock, or a SOL-serving
    // miner would carry a cooldown for a purse it never dropped.
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    light_tao_purse(&mut svm, &vals, &miner, TAO_AMOUNT as u64);

    send(&mut svm, deactivate_ix(&vals[0].pubkey(), &m, TAO), &vals[0].pubkey(), &vals[0]).expect("d0");
    send(&mut svm, deactivate_ix(&vals[1].pubkey(), &m, TAO), &vals[1].pubkey(), &vals[1]).expect("d1");

    let ms = miner_state(&svm, &m);
    assert_eq!(ms.active_backings, BACKING_BIT_SOL);
    assert_eq!(ms.deactivation_at, 0, "a TAO sweep is gated on the vault's unlock path, not this clock");
}

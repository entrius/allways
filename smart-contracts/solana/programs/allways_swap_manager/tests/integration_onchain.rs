// On-chain integration tests — real transactions to a LIVE `solana-test-validator` over RPC.
//
// Unlike the LiteSVM unit suite (in-process, clock-warpable), these exercise the *deployed*
// program at http://127.0.0.1:8899 via `solana-rpc-client`'s `RpcClient`. They reuse the exact
// instruction-building / PDA / keccak patterns from the unit tests, but submit, sign, and confirm
// real transactions and read state back with `get_account` + `try_deserialize`.
//
// Every test is `#[ignore]` so the default `cargo test` (validator-free LiteSVM suite) skips them.
// Run them ONLY against a live, freshly-reset validator with the program already deployed:
//
//   cargo test -p allways_swap_manager --test integration_onchain -- --ignored
//
// Requirements / caveats:
//   * A FRESH validator each run (`solana-test-validator --reset`). `initialize` creates singleton
//     Config/Vault PDAs; re-running against a dirty ledger will see them already present.
//   * Real wall-clock — the clock CANNOT be warped. So these cover the happy path and
//     time-independent guards only. Generous TTL/timeout values are used in `initialize` so nothing
//     expires mid-test. Reservation-expiry / swap-timeout stay in LiteSVM.
//   * Config + the 3-validator whitelist are shared singletons, set up exactly once (guarded by a
//     `OnceLock`). Each test uses FRESH miner/user keypairs so per-miner PDAs never collide and
//     tests can run in parallel.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::instruction::Instruction, AccountDeserialize,
        InstructionData, ToAccountMetas,
    },
    allways_swap_manager::constants::POOL_WINDOW_SECS,
    allways_swap_manager::state::{
        Config, MinerQuote, MinerState, Pool, Reservation, Swap, SwapStatus, TxMarker, Vault,
    },
    solana_commitment_config::CommitmentConfig,
    solana_keccak_hasher::hashv,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_native_token::LAMPORTS_PER_SOL,
    solana_rpc_client::rpc_client::RpcClient,
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
    std::sync::{Mutex, OnceLock},
};

const RPC_URL: &str = "http://127.0.0.1:8899";
const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;
const SLOT_HASHES_ID: Pubkey = Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111");

const REQ_ACTIVATE: u8 = 0;
const REQ_INITIATE: u8 = 2;
const REQ_CONFIRM: u8 = 6;
const REQ_SET_WEIGHTS: u8 = 8;

// Generous, wall-clock-proof bounds so nothing expires during a real-time run.
const TTL_SECS: i64 = 86_400; // reservation TTL: 1 day
const TIMEOUT_SECS: i64 = 86_400; // swap fulfillment timeout: 1 day
const THRESHOLD: u8 = 66; // 2-of-3 quorum
const MIN_COLLATERAL: u64 = LAMPORTS_PER_SOL; // 1 SOL
const COLLATERAL: u64 = 10 * LAMPORTS_PER_SOL; // 10 SOL
const SOL_AMOUNT: u64 = 2 * LAMPORTS_PER_SOL; // 2 SOL swap size

// Reservation quote — must be byte-identical reserve→initiate (it's hash-bound).
const FROM_ADDR: &str = "userBTCaddr";
const FROM_CHAIN: &str = "BTC";
const TO_CHAIN: &str = "SOL";
const MINER_FROM: &str = "minerBTCaddr";
const MINER_TO: &str = "minerSOLaddr";
const RATE: &str = "1.5";

// ─── PDA helpers (mirror the unit tests) ────────────────────────────────────────
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
fn vote_pda(req: u8, key: &[u8]) -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[req], key], &pid()).0
}
fn resv_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"resv", m.as_ref()], &pid()).0
}
fn pool_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"pool", m.as_ref()], &pid()).0
}
fn weights_round_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"vote", &[REQ_SET_WEIGHTS]], &pid()).0
}
fn swap_pda(key: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"swap", key], &pid()).0
}
fn tx_pda(key: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"tx", key], &pid()).0
}
fn swap_key(from_tx_hash: &str) -> [u8; 32] {
    hashv(&[from_tx_hash.as_bytes()]).to_bytes()
}

// ─── RPC plumbing ───────────────────────────────────────────────────────────────
fn rpc() -> RpcClient {
    RpcClient::new_with_commitment(RPC_URL.to_string(), CommitmentConfig::confirmed())
}

/// Airdrop `lamports` to `pk` and block until confirmed.
fn fund(rpc: &RpcClient, pk: &Pubkey, lamports: u64) {
    let sig = rpc
        .request_airdrop(pk, lamports)
        .unwrap_or_else(|e| panic!("airdrop to {pk} failed: {e}"));
    // Wait for the airdrop to confirm so subsequent txs see the balance.
    loop {
        if rpc.confirm_transaction(&sig).unwrap_or(false) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

/// A funded keypair, ready to sign.
fn funded_keypair(rpc: &RpcClient, lamports: u64) -> Keypair {
    let kp = Keypair::new();
    fund(rpc, &kp.pubkey(), lamports);
    kp
}

/// Build, sign, send + confirm a single-instruction tx. Returns Ok on success, Err(message) on
/// any client/program failure (so negative tests can assert `.is_err()`).
fn send(rpc: &RpcClient, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    let bh = rpc.get_latest_blockhash().map_err(|e| format!("blockhash: {e}"))?;
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &bh);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer])
        .map_err(|e| format!("sign: {e}"))?;
    rpc.send_and_confirm_transaction(&tx)
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

// ─── account readers ────────────────────────────────────────────────────────────
fn read_config(rpc: &RpcClient) -> Config {
    let a = rpc.get_account(&config_pda()).expect("config account");
    Config::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn read_vault(rpc: &RpcClient) -> Vault {
    let a = rpc.get_account(&vault_pda()).expect("vault account");
    Vault::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn vault_lamports(rpc: &RpcClient) -> u64 {
    rpc.get_account(&vault_pda()).map(|a| a.lamports).unwrap_or(0)
}
fn read_miner(rpc: &RpcClient, m: &Pubkey) -> MinerState {
    let a = rpc.get_account(&miner_pda(m)).expect("miner state account");
    MinerState::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn read_reservation(rpc: &RpcClient, m: &Pubkey) -> Reservation {
    let a = rpc.get_account(&resv_pda(m)).expect("reservation account");
    Reservation::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn read_swap(rpc: &RpcClient, key: &[u8; 32]) -> Swap {
    let a = rpc.get_account(&swap_pda(key)).expect("swap account");
    Swap::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn account_exists(rpc: &RpcClient, pk: &Pubkey) -> bool {
    rpc.get_account(pk).is_ok()
}

// ─── instruction builders (mirror the unit tests) ───────────────────────────────
fn init_ix(admin: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Initialize {
            min_collateral: MIN_COLLATERAL,
            max_collateral: 0,
            fulfillment_timeout_secs: TIMEOUT_SECS,
            consensus_threshold_percent: THRESHOLD,
            min_swap_amount: 0,
            max_swap_amount: 0,
            reservation_ttl_secs: TTL_SECS,
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
            vote_round: vote_pda(REQ_ACTIVATE, miner.as_ref()),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn open_ix(validator: &Pubkey, miner: &Pubkey, user: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::OpenOrRequest {
            from_chain: FROM_CHAIN.to_string(),
            to_chain: TO_CHAIN.to_string(),
            user: *user,
            user_from_addr: FROM_ADDR.to_string(),
            user_to_addr: "userSOLaddr".to_string(),
            sol_amount: SOL_AMOUNT,
            from_amount: 100_000,
            to_amount: 0,
        }
        .data(),
        allways_swap_manager::accounts::OpenOrRequest {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            quote: quote_pda(miner, FROM_CHAIN, TO_CHAIN),
            pool: pool_pda(miner),
            vault: vault_pda(),
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
/// Sleep just past the pool window so `resolve_pool` is callable (real wall-clock on the validator).
fn wait_pool_window() {
    std::thread::sleep(std::time::Duration::from_millis((POOL_WINDOW_SECS as u64) * 1000 + 1200));
}
fn initiate_ix(
    validator: &Pubkey,
    miner: &Pubkey,
    from_tx_hash: &str,
    user: &Pubkey,
    user_to: &str,
) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteInitiate {
            swap_key: key,
            from_tx_hash: from_tx_hash.to_string(),
            from_tx_block: 800_000,
            user: *user,
            user_from_address: FROM_ADDR.to_string(),
            user_to_address: user_to.to_string(),
        }
        .data(),
        allways_swap_manager::accounts::VoteInitiate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner),
            vote_round: vote_pda(REQ_INITIATE, miner.as_ref()),
            tx_marker: tx_pda(&key),
            swap: swap_pda(&key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn fulfill_ix(miner: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::MarkFulfilled {
            swap_key: key,
            to_tx_hash: "destTxHash".to_string(),
            to_tx_block: 200,
        }
        .data(),
        allways_swap_manager::accounts::MarkFulfilled { miner: *miner, swap: swap_pda(&key) }
            .to_account_metas(None),
    )
}
fn confirm_ix(validator: &Pubkey, miner: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::ConfirmSwap { swap_key: key }.data(),
        allways_swap_manager::accounts::ConfirmSwap {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            vault: vault_pda(),
            swap: swap_pda(&key),
            vote_round: vote_pda(REQ_CONFIRM, &key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn withdraw_treasury_ix(admin: &Pubkey, recipient: &Pubkey, amount: u64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::WithdrawTreasury { amount }.data(),
        allways_swap_manager::accounts::WithdrawTreasury {
            admin: *admin,
            config: config_pda(),
            vault: vault_pda(),
            recipient: *recipient,
        }
        .to_account_metas(None),
    )
}
fn vote_weights_ix(validator: &Pubkey, weights: Vec<u64>) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteSetWeights { weights }.data(),
        allways_swap_manager::accounts::VoteSetWeights {
            validator: *validator,
            config: config_pda(),
            vote_round: weights_round_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

// ─── shared one-time setup: Config + 3-validator whitelist ───────────────────────
//
// `initialize` + `add_validator` touch the singleton Config, so they must run exactly once *per
// ledger*. The e2e harness surfaces each integration test individually, i.e. as a SEPARATE
// `cargo test` PROCESS — so a process-global isn't enough: every process must rebuild the SAME
// admin + validator identities and treat the singleton setup as idempotent (skip init/add_validator
// if already on-chain). We therefore derive them from FIXED 32-byte seeds (deterministic across
// processes) and fund them via airdrop (idempotent top-up). A process-local `OnceLock` then runs the
// (idempotent) on-chain setup at most once per process and caches the keypairs.
//
// This is also why the suite needs a FRESH `--reset` ledger when run as a whole (the very first
// `initialize` must succeed); subsequent tests/processes against that same live ledger reuse it.
const ADMIN_SEED: [u8; 32] = [7u8; 32];
fn validator_seed(i: u8) -> [u8; 32] {
    // Distinct, deterministic per-validator seed (0x41, 0x42, 0x43 for i = 0,1,2).
    [0x41u8.wrapping_add(i); 32]
}

struct Shared {
    admin: Keypair,
    validators: Vec<Keypair>,
}

fn shared() -> &'static Shared {
    static SHARED: OnceLock<Shared> = OnceLock::new();
    static SETUP_LOCK: Mutex<()> = Mutex::new(());
    SHARED.get_or_init(|| {
        let _guard = SETUP_LOCK.lock().unwrap();
        let rpc = rpc();

        // Deterministic identities (same across every test process on this ledger).
        let admin = Keypair::new_from_array(ADMIN_SEED);
        let validators: Vec<Keypair> =
            (0..3).map(|i| Keypair::new_from_array(validator_seed(i))).collect();

        // Fund (idempotent top-up — safe to repeat across processes).
        fund(&rpc, &admin.pubkey(), 100 * LAMPORTS_PER_SOL);
        for v in &validators {
            fund(&rpc, &v.pubkey(), 100 * LAMPORTS_PER_SOL);
        }

        // Initialize the singleton Config + Vault iff not already present (idempotent).
        if !account_exists(&rpc, &config_pda()) {
            send(&rpc, init_ix(&admin.pubkey()), &admin.pubkey(), &admin)
                .expect("initialize singleton Config/Vault (needs a FRESH --reset validator)");
        }

        // Whitelist the 3 validators iff not already in the set (idempotent).
        let cfg = read_config(&rpc);
        for v in &validators {
            if !cfg.validators.iter().any(|x| x.key == v.pubkey()) {
                send(&rpc, add_validator_ix(&admin.pubkey(), v.pubkey()), &admin.pubkey(), &admin)
                    .expect("add_validator");
            }
        }

        Shared { admin, validators }
    })
}

fn admin_keypair() -> Keypair {
    shared().admin.insecure_clone()
}
fn validator_keypairs() -> Vec<Keypair> {
    shared().validators.iter().map(|v| v.insecure_clone()).collect()
}

/// Create a fresh, funded, *active* miner with a posted collateral of `COLLATERAL`.
/// Returns the miner keypair. Uses the shared 3-validator set for the 2-of-3 activation quorum.
fn active_miner(rpc: &RpcClient) -> Keypair {
    let vals = validator_keypairs();
    let miner = funded_keypair(rpc, 100 * LAMPORTS_PER_SOL);
    send(rpc, post_ix(&miner.pubkey(), COLLATERAL), &miner.pubkey(), &miner).expect("post");
    send(rpc, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0])
        .expect("activate v0");
    send(rpc, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1])
        .expect("activate v1");
    miner
}

/// Active miner + a confirmed reservation via the lottery (post quote → open pool → wait window →
/// resolve; sole entrant wins deterministically).
fn reserved_miner(rpc: &RpcClient) -> Keypair {
    let vals = validator_keypairs();
    let miner = active_miner(rpc);
    let user = Keypair::new().pubkey();
    send(rpc, set_quote_ix(&miner.pubkey(), FROM_CHAIN, TO_CHAIN, RATE), &miner.pubkey(), &miner).expect("set_quote");
    send(rpc, open_ix(&vals[0].pubkey(), &miner.pubkey(), &user), &vals[0].pubkey(), &vals[0])
        .expect("open pool");
    wait_pool_window();
    send(rpc, resolve_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0])
        .expect("resolve pool");
    miner
}

/// Vault invariant: lamports == rent_reserve + total_collateral + treasury_total.
/// We don't know the rent reserve a priori (the vault is a shared singleton), so the caller passes
/// it (captured once, right after the vault exists).
fn invariant_holds(rpc: &RpcClient, rent_reserve: u64) -> bool {
    let v = read_vault(rpc);
    vault_lamports(rpc) == rent_reserve + v.total_collateral + v.treasury_total
}

// ════════════════════════════════════════════════════════════════════════════════
//  TESTS  (all #[ignore] — live-validator only)
// ════════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_initialize_creates_config() {
    let _ = shared(); // ensure init ran
    let rpc = rpc();
    let admin = admin_keypair();
    let cfg = read_config(&rpc);
    assert_eq!(cfg.admin, admin.pubkey(), "admin recorded");
    assert_eq!(cfg.version, 4, "schema version");
    assert_eq!(cfg.min_collateral, MIN_COLLATERAL);
    assert_eq!(cfg.consensus_threshold_percent, THRESHOLD);
    assert_eq!(cfg.fulfillment_timeout_secs, TIMEOUT_SECS);
    assert_eq!(cfg.reservation_ttl_secs, TTL_SECS);
    // Vault exists and is a valid singleton.
    let _ = read_vault(&rpc);
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_add_three_validators() {
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    let cfg = read_config(&rpc);
    assert!(cfg.validators.len() >= 3, "at least 3 validators whitelisted");
    for v in &vals {
        assert!(cfg.validators.iter().any(|x| x.key == v.pubkey()), "validator {} in set", v.pubkey());
    }
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_vote_set_weights_quorum() {
    // Must run before any test that grows the validator set (so quorum = 2 of 3 here).
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    let cfg = read_config(&rpc);
    let n = cfg.validators.len();

    // Full vector index-aligned to Config.validators; bump index 0 to a distinguishable value.
    let mut weights: Vec<u64> = cfg.validators.iter().map(|v| v.weight).collect();
    weights[0] = 42;

    send(&rpc, vote_weights_ix(&vals[0].pubkey(), weights.clone()), &vals[0].pubkey(), &vals[0])
        .expect("weights v0");
    send(&rpc, vote_weights_ix(&vals[1].pubkey(), weights.clone()), &vals[1].pubkey(), &vals[1])
        .expect("weights v1");

    let after = read_config(&rpc);
    assert_eq!(after.validators.len(), n, "set size unchanged");
    assert_eq!(after.validators[0].weight, 42, "consensus weight applied");
    assert!(after.last_weights_update > 0, "cadence stamp set");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_post_collateral() {
    let _ = shared();
    let rpc = rpc();
    let before = read_vault(&rpc).total_collateral;

    let miner = funded_keypair(&rpc, 100 * LAMPORTS_PER_SOL);
    send(&rpc, post_ix(&miner.pubkey(), COLLATERAL), &miner.pubkey(), &miner).expect("post");

    // MinerState reflects the deposit.
    let ms = read_miner(&rpc, &miner.pubkey());
    assert_eq!(ms.miner, miner.pubkey());
    assert_eq!(ms.collateral, COLLATERAL, "miner collateral credited");
    assert!(!ms.active, "not active yet");
    // Vault total_collateral grew by exactly COLLATERAL (post-total semantics: the new global total
    // equals the prior total plus this deposit).
    assert_eq!(read_vault(&rpc).total_collateral, before + COLLATERAL, "vault post-total");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_vote_activate_quorum() {
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    let miner = funded_keypair(&rpc, 100 * LAMPORTS_PER_SOL);
    send(&rpc, post_ix(&miner.pubkey(), COLLATERAL), &miner.pubkey(), &miner).expect("post");

    // One vote: below the 2-of-3 quorum, still inactive.
    send(&rpc, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0])
        .expect("activate v0");
    assert!(!read_miner(&rpc, &miner.pubkey()).active, "1/3 < quorum, inactive");

    // Second vote reaches quorum → active.
    send(&rpc, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1])
        .expect("activate v1");
    assert!(read_miner(&rpc, &miner.pubkey()).active, "2/3 >= quorum, active");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_pool_open_pins_quote() {
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    let miner = active_miner(&rpc);
    let user = Keypair::new().pubkey();

    send(&rpc, set_quote_ix(&miner.pubkey(), FROM_CHAIN, TO_CHAIN, RATE), &miner.pubkey(), &miner).expect("set_quote");
    send(&rpc, open_ix(&vals[0].pubkey(), &miner.pubkey(), &user), &vals[0].pubkey(), &vals[0])
        .expect("open pool");

    let p: Pool = {
        let a = rpc.get_account(&pool_pda(&miner.pubkey())).expect("pool account");
        Pool::try_deserialize(&mut a.data.as_slice()).unwrap()
    };
    assert_eq!(p.from_chain, FROM_CHAIN);
    assert_eq!(p.to_chain, TO_CHAIN);
    assert_eq!(p.miner_from_addr, MINER_FROM, "pinned from the on-chain quote");
    assert_eq!(p.miner_to_addr, MINER_TO);
    assert_eq!(p.rate, RATE);
    assert_eq!(p.requests.len(), 1);
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_reservation_fee_to_treasury() {
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    let miner = active_miner(&rpc);
    let user = Keypair::new().pubkey();
    send(&rpc, set_quote_ix(&miner.pubkey(), FROM_CHAIN, TO_CHAIN, RATE), &miner.pubkey(), &miner).expect("set_quote");

    let before = read_vault(&rpc).treasury_total;
    send(&rpc, open_ix(&vals[0].pubkey(), &miner.pubkey(), &user), &vals[0].pubkey(), &vals[0])
        .expect("open pool");
    let fee = allways_swap_manager::constants::RESERVATION_FEE_LAMPORTS;
    assert_eq!(
        read_vault(&rpc).treasury_total,
        before + fee,
        "flat reservation fee accrued to treasury"
    );
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_resolve_pool_creates_reservation() {
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    let miner = active_miner(&rpc);
    let u0 = Keypair::new().pubkey();
    let u1 = Keypair::new().pubkey();

    // Two validators contend; real SlotHashes seeds the weighted draw.
    send(&rpc, set_quote_ix(&miner.pubkey(), FROM_CHAIN, TO_CHAIN, RATE), &miner.pubkey(), &miner).expect("set_quote");
    send(&rpc, open_ix(&vals[0].pubkey(), &miner.pubkey(), &u0), &vals[0].pubkey(), &vals[0])
        .expect("open");
    send(&rpc, open_ix(&vals[1].pubkey(), &miner.pubkey(), &u1), &vals[1].pubkey(), &vals[1])
        .expect("join");
    wait_pool_window();
    send(&rpc, resolve_ix(&vals[2].pubkey(), &miner.pubkey()), &vals[2].pubkey(), &vals[2])
        .expect("resolve");

    let r = read_reservation(&rpc, &miner.pubkey());
    assert!(r.reserved_until > 0, "a winner was reserved");
    assert_eq!(r.from_chain, FROM_CHAIN);
    assert_eq!(r.to_chain, TO_CHAIN);
    assert_eq!(r.sol_amount, SOL_AMOUNT);
    assert_eq!(r.miner_from_addr, MINER_FROM, "pinned miner quote carried in");
    assert_eq!(r.miner_to_addr, MINER_TO);
    assert_eq!(r.rate, RATE);
    assert_eq!(r.from_addr, FROM_ADDR, "winner's user source addr");
    // pool reset for reuse
    let p: Pool = {
        let a = rpc.get_account(&pool_pda(&miner.pubkey())).expect("pool account");
        Pool::try_deserialize(&mut a.data.as_slice()).unwrap()
    };
    assert_eq!(p.opened_at, 0, "pool reset after resolve");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_vote_initiate_creates_swap() {
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    let miner = reserved_miner(&rpc);
    let user = Keypair::new().pubkey();
    let tx = "srctx_initiate";

    send(&rpc, initiate_ix(&vals[0].pubkey(), &miner.pubkey(), tx, &user, "userSOLaddr"),
        &vals[0].pubkey(), &vals[0]).expect("initiate v0");
    send(&rpc, initiate_ix(&vals[1].pubkey(), &miner.pubkey(), tx, &user, "userSOLaddr"),
        &vals[1].pubkey(), &vals[1]).expect("initiate v1");

    let key = swap_key(tx);
    let s = read_swap(&rpc, &key);
    assert_eq!(s.user, user);
    assert_eq!(s.miner, miner.pubkey());
    assert_eq!(s.sol_amount, SOL_AMOUNT);
    assert_eq!(s.miner_from_addr, MINER_FROM, "miner quote from reservation");
    assert_eq!(s.miner_to_addr, MINER_TO);
    assert_eq!(s.rate, RATE);
    assert_eq!(s.user_to_addr, "userSOLaddr");
    assert!(s.status == SwapStatus::Active, "swap starts Active");

    // TxMarker.used set (permanent replay guard).
    let tm = rpc.get_account(&tx_pda(&key)).expect("tx_marker");
    let marker = TxMarker::try_deserialize(&mut tm.data.as_slice()).unwrap();
    assert!(marker.used, "tx marker used");

    // miner now has an in-flight swap.
    assert!(read_miner(&rpc, &miner.pubkey()).has_active_swap, "miner has_active_swap");
    // reservation consumed (slot cleared).
    let r = read_reservation(&rpc, &miner.pubkey());
    assert_eq!(r.reserved_until, 0, "reservation consumed by initiate");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_mark_fulfilled() {
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    let miner = reserved_miner(&rpc);
    let user = Keypair::new().pubkey();
    let tx = "srctx_fulfill";

    send(&rpc, initiate_ix(&vals[0].pubkey(), &miner.pubkey(), tx, &user, "userSOLaddr"),
        &vals[0].pubkey(), &vals[0]).expect("initiate v0");
    send(&rpc, initiate_ix(&vals[1].pubkey(), &miner.pubkey(), tx, &user, "userSOLaddr"),
        &vals[1].pubkey(), &vals[1]).expect("initiate v1");

    send(&rpc, fulfill_ix(&miner.pubkey(), tx), &miner.pubkey(), &miner).expect("fulfill");

    let key = swap_key(tx);
    let s = read_swap(&rpc, &key);
    assert!(s.status == SwapStatus::Fulfilled, "status Fulfilled after mark_fulfilled");
    assert_eq!(s.to_tx_hash, "destTxHash");
    assert!(s.fulfilled_at > 0, "fulfilled_at set");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_confirm_swap_full_lifecycle() {
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    // Capture the vault rent reserve = lamports - (collateral + treasury) before our swap.
    let rent_reserve = {
        let v = read_vault(&rpc);
        vault_lamports(&rpc) - v.total_collateral - v.treasury_total
    };
    assert!(invariant_holds(&rpc, rent_reserve), "vault invariant pre-flow");

    let miner = reserved_miner(&rpc);
    let user = Keypair::new().pubkey();
    let tx = "srctx_confirm";

    send(&rpc, initiate_ix(&vals[0].pubkey(), &miner.pubkey(), tx, &user, "userSOLaddr"),
        &vals[0].pubkey(), &vals[0]).expect("initiate v0");
    send(&rpc, initiate_ix(&vals[1].pubkey(), &miner.pubkey(), tx, &user, "userSOLaddr"),
        &vals[1].pubkey(), &vals[1]).expect("initiate v1");
    send(&rpc, fulfill_ix(&miner.pubkey(), tx), &miner.pubkey(), &miner).expect("fulfill");

    let coll_before = read_miner(&rpc, &miner.pubkey()).collateral;
    let treasury_before = read_vault(&rpc).treasury_total;

    send(&rpc, confirm_ix(&vals[0].pubkey(), &miner.pubkey(), tx), &vals[0].pubkey(), &vals[0])
        .expect("confirm v0");
    send(&rpc, confirm_ix(&vals[1].pubkey(), &miner.pubkey(), tx), &vals[1].pubkey(), &vals[1])
        .expect("confirm v1");

    let fee = SOL_AMOUNT / 100; // 1%
    // Fee taken from collateral, accrued to treasury (post-total semantics).
    assert_eq!(read_miner(&rpc, &miner.pubkey()).collateral, coll_before - fee, "1% fee from collateral");
    assert_eq!(read_vault(&rpc).treasury_total, treasury_before + fee, "fee accrued to treasury");
    // Swap closed, miner freed.
    assert!(!account_exists(&rpc, &swap_pda(&swap_key(tx))), "swap account closed");
    assert!(!read_miner(&rpc, &miner.pubkey()).has_active_swap, "miner freed");
    // Vault invariant holds after the fee move.
    assert!(invariant_holds(&rpc, rent_reserve), "vault invariant after fee");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_withdraw_treasury_happy_path() {
    let _ = shared();
    let rpc = rpc();
    let vals = validator_keypairs();
    let admin = admin_keypair();

    // Accrue a fee via a full swap+confirm.
    let miner = reserved_miner(&rpc);
    let user = Keypair::new().pubkey();
    let tx = "srctx_withdraw";
    send(&rpc, initiate_ix(&vals[0].pubkey(), &miner.pubkey(), tx, &user, "userSOLaddr"),
        &vals[0].pubkey(), &vals[0]).expect("initiate v0");
    send(&rpc, initiate_ix(&vals[1].pubkey(), &miner.pubkey(), tx, &user, "userSOLaddr"),
        &vals[1].pubkey(), &vals[1]).expect("initiate v1");
    send(&rpc, fulfill_ix(&miner.pubkey(), tx), &miner.pubkey(), &miner).expect("fulfill");
    send(&rpc, confirm_ix(&vals[0].pubkey(), &miner.pubkey(), tx), &vals[0].pubkey(), &vals[0])
        .expect("confirm v0");
    send(&rpc, confirm_ix(&vals[1].pubkey(), &miner.pubkey(), tx), &vals[1].pubkey(), &vals[1])
        .expect("confirm v1");

    let fee = SOL_AMOUNT / 100;
    let treasury_before = read_vault(&rpc).treasury_total;
    assert!(treasury_before >= fee, "treasury holds at least this swap's fee");

    let recipient = Keypair::new().pubkey();
    let recip_before = rpc.get_account(&recipient).map(|a| a.lamports).unwrap_or(0);
    send(&rpc, withdraw_treasury_ix(&admin.pubkey(), &recipient, fee), &admin.pubkey(), &admin)
        .expect("withdraw treasury");

    assert_eq!(
        rpc.get_account(&recipient).map(|a| a.lamports).unwrap_or(0),
        recip_before + fee,
        "recipient received the fee"
    );
    assert_eq!(read_vault(&rpc).treasury_total, treasury_before - fee, "treasury drained by fee");
}

// ─── reachable negative cases (time-independent guards) ──────────────────────────

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_non_validator_vote_activate_rejected() {
    let _ = shared();
    let rpc = rpc();
    let miner = funded_keypair(&rpc, 100 * LAMPORTS_PER_SOL);
    send(&rpc, post_ix(&miner.pubkey(), COLLATERAL), &miner.pubkey(), &miner).expect("post");

    // An outsider (not in the validator whitelist) tries to vote → rejected.
    let outsider = funded_keypair(&rpc, 10 * LAMPORTS_PER_SOL);
    let res = send(&rpc, vote_activate_ix(&outsider.pubkey(), &miner.pubkey()),
        &outsider.pubkey(), &outsider);
    assert!(res.is_err(), "non-validator vote_activate must be rejected");
    assert!(!read_miner(&rpc, &miner.pubkey()).active, "miner stays inactive");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_non_admin_withdraw_treasury_rejected() {
    let _ = shared();
    let rpc = rpc();
    // A non-admin attempts a treasury withdrawal → rejected by the admin guard.
    let outsider = funded_keypair(&rpc, 10 * LAMPORTS_PER_SOL);
    let recipient = Keypair::new().pubkey();
    let treasury_before = read_vault(&rpc).treasury_total;
    let res = send(&rpc, withdraw_treasury_ix(&outsider.pubkey(), &recipient, 1),
        &outsider.pubkey(), &outsider);
    assert!(res.is_err(), "non-admin withdraw_treasury must be rejected");
    assert_eq!(read_vault(&rpc).treasury_total, treasury_before, "treasury untouched");
}

// ─── Phase 8: miner quotes + validator weights ───────────────────────────────────

fn quote_pda(m: &Pubkey, from_chain: &str, to_chain: &str) -> Pubkey {
    Pubkey::find_program_address(
        &[b"quote", m.as_ref(), from_chain.as_bytes(), to_chain.as_bytes()],
        &pid(),
    )
    .0
}

fn set_quote_ix(m: &Pubkey, from_chain: &str, to_chain: &str, rate: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetQuote {
            from_chain: from_chain.to_string(),
            to_chain: to_chain.to_string(),
            miner_from_addr: MINER_FROM.to_string(),
            miner_to_addr: MINER_TO.to_string(),
            rate: rate.to_string(),
            liquidity: 1_000,
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *m,
            quote: quote_pda(m, from_chain, to_chain),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

fn remove_quote_ix(m: &Pubkey, from_chain: &str, to_chain: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::RemoveQuote {
            from_chain: from_chain.to_string(),
            to_chain: to_chain.to_string(),
        }
        .data(),
        allways_swap_manager::accounts::RemoveQuote { miner: *m, quote: quote_pda(m, from_chain, to_chain) }
            .to_account_metas(None),
    )
}

fn set_validator_weight_ix(admin: &Pubkey, v: Pubkey, weight: u64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetValidatorWeight { validator: v, weight }.data(),
        allways_swap_manager::accounts::AdminConfig { admin: *admin, config: config_pda() }
            .to_account_metas(None),
    )
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_set_quote_creates_pda() {
    let _ = shared();
    let rpc = rpc();
    let miner = funded_keypair(&rpc, 10 * LAMPORTS_PER_SOL);

    send(&rpc, set_quote_ix(&miner.pubkey(), "BTC", "SOL", "1.5"), &miner.pubkey(), &miner)
        .expect("set_quote");

    let a = rpc.get_account(&quote_pda(&miner.pubkey(), "BTC", "SOL")).expect("quote account");
    let q = MinerQuote::try_deserialize(&mut a.data.as_slice()).unwrap();
    assert_eq!(q.miner, miner.pubkey());
    assert_eq!(q.from_chain, "BTC");
    assert_eq!(q.to_chain, "SOL");
    assert_eq!(q.rate, "1.5");
    assert!(q.updated_at > 0, "updated_at set from on-chain clock");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_remove_quote_closes_pda() {
    let _ = shared();
    let rpc = rpc();
    let miner = funded_keypair(&rpc, 10 * LAMPORTS_PER_SOL);

    send(&rpc, set_quote_ix(&miner.pubkey(), "BTC", "SOL", "1.5"), &miner.pubkey(), &miner).expect("set");
    assert!(account_exists(&rpc, &quote_pda(&miner.pubkey(), "BTC", "SOL")), "quote exists after set");

    send(&rpc, remove_quote_ix(&miner.pubkey(), "BTC", "SOL"), &miner.pubkey(), &miner).expect("remove");
    assert!(!account_exists(&rpc, &quote_pda(&miner.pubkey(), "BTC", "SOL")), "quote closed after remove");
}

#[test]
#[ignore = "requires a live solana-test-validator with the program deployed"]
fn onchain_set_validator_weight() {
    let _ = shared();
    let rpc = rpc();
    let admin = admin_keypair();
    let v = Keypair::new();

    // Add a fresh validator with weight 5, then bump it to 9 via set_validator_weight.
    send(&rpc, add_validator_ix(&admin.pubkey(), v.pubkey()), &admin.pubkey(), &admin).expect("add (weight 1)");
    send(&rpc, set_validator_weight_ix(&admin.pubkey(), v.pubkey(), 9), &admin.pubkey(), &admin).expect("set weight");

    let cfg = read_config(&rpc);
    let w = cfg.validators.iter().find(|x| x.key == v.pubkey()).map(|x| x.weight);
    assert_eq!(w, Some(9), "validator weight updated to 9");
}

// W1 — split-collateral backing seam: `collateral_chain` on Reservation/Swap, per-backing swap bounds,
// and the verdict-only timeout path (LiteSVM, clock-controlled).
//   cargo test -p allways_swap_manager --test test_backing
//
// No instruction can pin a non-"sol" backing yet (quote-level declaration is W2), so the tests that
// exercise the seam write the backing straight into the account and re-serialize it — the same state a
// TAO-backed quote will produce, reached the only way W1 can reach it.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, AccountSerialize, AnchorDeserialize, Discriminator, InstructionData,
        ToAccountMetas,
    },
    allways_swap_manager::constants::{
        required_collateral, POOL_WINDOW_SECS, SETTLEMENT_GRACE_SECS, TAO_MIN_SWAP_AMOUNT_RAO,
    },
    allways_swap_manager::events::SwapTimedOut,
    allways_swap_manager::state::{Config, MinerState, Pool, Reservation, Swap},
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
const REQ_TIMEOUT: u8 = 7;
const BASE_TS: i64 = 1_700_000_000;
const TTL: i64 = 1_800;
const TIMEOUT_SECS: i64 = 3_600;
const COLLATERAL: u64 = 10_000_000_000; // 10 SOL
const SOL_AMOUNT: u64 = 2_000_000_000; // 2 SOL — the collateral-denominated leg on the "sol" path
const TAO_AMOUNT: u128 = 500_000_000; // 0.5 TAO (rao), inside the deployed [0.1, 1] τ band
const OTHER_AMOUNT: u128 = 1_333_333_333; // the non-collateral leg (asset-native units)
const FROM_TX_BLOCK: u32 = 800_000;
const LOTTERY_USER: Pubkey = Pubkey::new_from_array([7u8; 32]);

// Spoke pair (BTC→SOL): the SOL leg is the destination, so "sol" backing binds to `to_amount`.
const SPOKE_FROM: &str = "btc";
const SPOKE_TO: &str = "sol";
// Hub↔hub pair (SOL→TAO): "sol" backing binds to `from_amount`, "tao" backing to `to_amount` — same
// leg lookup, opposite side. This pair is what proves the guards never branch on the pair.
const HUB_FROM: &str = "sol";
const HUB_TO: &str = "tao";
const MINER_FROM: &str = "minerSrcAddr";
const MINER_TO: &str = "minerDstAddr";
const RATE: u128 = 1_500_000_000_000_000_000; // 1.5 × RATE_PRECISION

fn pid() -> Pubkey {
    allways_swap_manager::id()
}
fn config_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"config"], &pid()).0
}
fn collateral_vault_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"collateral", m.as_ref()], &pid()).0
}
fn treasury_pda() -> Pubkey {
    Pubkey::find_program_address(&[b"treasury"], &pid()).0
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
    send_meta(svm, ix, payer, signer).map(|_| ())
}
/// As `send`, but keeps the transaction logs — the only place emitted events are observable.
fn send_meta(
    svm: &mut LiteSVM,
    ix: Instruction,
    payer: &Pubkey,
    signer: &Keypair,
) -> Result<Vec<String>, String> {
    svm.expire_blockhash();
    let bh = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &bh);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx)
        .map(|m| m.logs)
        .map_err(|e| format!("{:?}", e))
}

fn init_ix(admin: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::Initialize {
            min_collateral: 1_000_000_000,
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
fn vote_activate_ix(validator: &Pubkey, miner: &Pubkey) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteActivate { backing: "sol".to_string() }.data(),
        allways_swap_manager::accounts::VoteActivate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            vote_round: vote_pda(REQ_ACTIVATE, miner.as_ref()),
            attestation: None,
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn set_quote_ix(miner: &Pubkey, f: &str, t: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SetQuote {
            from_chain: f.to_string(),
            to_chain: t.to_string(),
            collateral_chain: BACKING.to_string(),
            miner_from_addr: MINER_FROM.to_string(),
            miner_to_addr: MINER_TO.to_string(),
            rate: RATE,
            liquidity: 1_000,
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *miner,
            miner_state: miner_pda(miner),
            quote: quote_pda(miner, f, t, BACKING),
            treasury: treasury_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn open_ix(router: &Pubkey, miner: &Pubkey, f: &str, t: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::OpenOrRequest {
            from_chain: f.to_string(),
            to_chain: t.to_string(),
            collateral_chain: BACKING.to_string(),
        }
        .data(),
        allways_swap_manager::accounts::OpenOrRequest {
            router: *router,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            quote: quote_pda(miner, f, t, BACKING),
            attestation: None,
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
    user: &Pubkey,
    collateral_amount: u64,
    from_amount: u128,
    to_amount: u128,
) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::FinalizeReservation {
            user: *user,
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
            attestation: None,
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
fn initiate_ix(validator: &Pubkey, miner: &Pubkey, from_tx_hash: &str) -> Instruction {
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
            attestation: None,
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn timeout_ix(validator: &Pubkey, miner: &Pubkey, user: &Pubkey, from_tx_hash: &str) -> Instruction {
    let key = swap_key(from_tx_hash);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::TimeoutSwap { swap_key: key }.data(),
        allways_swap_manager::accounts::TimeoutSwap {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            collateral_vault: collateral_vault_pda(miner),
            user: *user,
            swap: swap_pda(&key),
            vote_round: vote_pda(REQ_TIMEOUT, &key),
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
fn reservation_acct(svm: &LiteSVM, m: &Pubkey) -> Reservation {
    Reservation::try_deserialize(&mut svm.get_account(&resv_pda(m)).unwrap().data.as_slice()).unwrap()
}
fn swap_acct(svm: &LiteSVM, key: &[u8; 32]) -> Swap {
    Swap::try_deserialize(&mut svm.get_account(&swap_pda(key)).unwrap().data.as_slice()).unwrap()
}
fn lamports(svm: &LiteSVM, p: &Pubkey) -> u64 {
    svm.get_account(p).map(|a| a.lamports).unwrap_or(0)
}

/// Rewrite an existing account in place, preserving its allocated length (accounts are fixed-size).
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
    let miner = s.miner;
    s.collateral_chain = backing.to_string();
    let mut buf = Vec::new();
    s.try_serialize(&mut buf).unwrap();
    overwrite(svm, swap_pda(key), buf);
    // Re-home the miner's per-hub swap bit + busy slot too, so the backdoor edit reads as "this swap
    // was always backed there" — what vote_initiate would have written for that backing. An
    // unsupported backing has no bit to re-home to; those tests only need the Swap field flipped.
    let Ok(new_bit) = allways_swap_manager::backing::backing_bit(backing) else {
        return;
    };
    let mut ms = miner_state(svm, &miner);
    if ms.active_swap_backings != new_bit {
        let busy = ms.busy_any_until();
        for (bit, _) in allways_swap_manager::constants::BACKINGS {
            if ms.active_swap_backings & bit != 0 {
                ms.set_swap(bit, false);
                ms.set_busy(bit, 0);
            }
        }
        ms.set_swap(new_bit, true);
        ms.set_busy(new_bit, busy);
        let mut buf = Vec::new();
        ms.try_serialize(&mut buf).unwrap();
        overwrite(svm, miner_pda(&miner), buf);
    }
}

/// Standard-alphabet base64 decode — just enough for `Program data:` log lines.
fn b64(s: &str) -> Vec<u8> {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let (mut acc, mut bits, mut out) = (0u32, 0u32, Vec::new());
    for c in s.bytes().filter(|&c| c != b'=') {
        let v = A.iter().position(|&a| a == c).expect("base64 alphabet") as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}
fn timed_out_event(logs: &[String]) -> SwapTimedOut {
    for l in logs {
        if let Some(payload) = l.strip_prefix("Program data: ") {
            let raw = b64(payload);
            if raw.len() > 8 && raw[..8] == SwapTimedOut::DISCRIMINATOR[..] {
                return SwapTimedOut::deserialize(&mut &raw[8..]).unwrap();
            }
        }
    }
    panic!("no SwapTimedOut event in logs: {:?}", logs);
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

/// init + 3 validators + an activated miner holding `COLLATERAL`, quotes posted on both pairs.
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
    send(&mut svm, vote_activate_ix(&vals[0].pubkey(), &miner.pubkey()), &vals[0].pubkey(), &vals[0]).expect("a0");
    send(&mut svm, vote_activate_ix(&vals[1].pubkey(), &miner.pubkey()), &vals[1].pubkey(), &vals[1]).expect("a1");
    send(&mut svm, set_quote_ix(&miner.pubkey(), SPOKE_FROM, SPOKE_TO), &miner.pubkey(), &miner).expect("quote spoke");
    send(&mut svm, set_quote_ix(&miner.pubkey(), HUB_FROM, HUB_TO), &miner.pubkey(), &miner).expect("quote hub");

    (svm, admin, vals, miner)
}

/// Bid → draw on `pair`, leaving an UNFILLED reservation (the clock lands past the pool window).
fn draw(svm: &mut LiteSVM, val: &Keypair, miner: &Pubkey, f: &str, t: &str) {
    let now = now_ts(svm);
    send(svm, open_ix(&val.pubkey(), miner, f, t), &val.pubkey(), val).expect("open");
    set_clock(svm, now + POOL_WINDOW_SECS + 1);
    arm_and_resolve(svm, val, miner);
}

/// Full BTC→SOL reservation, filled with the standard SOL-denominated amounts.
fn reserve_spoke(svm: &mut LiteSVM, val: &Keypair, miner: &Pubkey) {
    draw(svm, val, miner, SPOKE_FROM, SPOKE_TO);
    send(
        svm,
        finalize_ix(&val.pubkey(), miner, &LOTTERY_USER, SOL_AMOUNT, OTHER_AMOUNT, SOL_AMOUNT as u128),
        &val.pubkey(),
        val,
    )
    .expect("finalize");
}

fn do_initiate(svm: &mut LiteSVM, vals: &[Keypair], miner: &Pubkey, tx: &str) {
    send(svm, claim_ix(&vals[0].pubkey(), miner, tx), &vals[0].pubkey(), &vals[0]).expect("claim");
    send(svm, initiate_ix(&vals[0].pubkey(), miner, tx), &vals[0].pubkey(), &vals[0]).expect("i0");
    send(svm, initiate_ix(&vals[1].pubkey(), miner, tx), &vals[1].pubkey(), &vals[1]).expect("i1");
}

#[test]
fn test_backing_defaults_to_sol_and_is_pinned_through_the_swap() {
    // The P0 seam self-describes the collateral: pinned at the draw, copied to the Swap at claim, and
    // never rewritten afterwards (attesting the claim must not be able to change what backs it).
    let (mut svm, _admin, vals, miner) = setup();
    draw(&mut svm, &vals[0], &miner.pubkey(), SPOKE_FROM, SPOKE_TO);
    assert_eq!(
        reservation_acct(&svm, &miner.pubkey()).collateral_chain,
        "sol",
        "the draw pins the backing"
    );

    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, SOL_AMOUNT, OTHER_AMOUNT, SOL_AMOUNT as u128),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("finalize");
    send(&mut svm, claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("claim");

    let key = swap_key("srctx1");
    assert_eq!(swap_acct(&svm, &key).collateral_chain, "sol", "copied Reservation → Swap");
    send(&mut svm, initiate_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("i0");
    send(&mut svm, initiate_ix(&vals[1].pubkey(), &miner.pubkey(), "srctx1"), &vals[1].pubkey(), &vals[1]).expect("i1");
    assert_eq!(swap_acct(&svm, &key).collateral_chain, "sol", "immutable across attestation");
}

#[test]
fn test_sol_backed_timeout_still_slashes_refunds_and_frees() {
    // The acceptance bar: with everything defaulting to "sol", timeout behaves exactly as before —
    // atomic 1.10× slash, full refund to the user, miner freed instantly (busy_until back to 0).
    let (mut svm, _admin, vals, miner) = setup();
    reserve_spoke(&mut svm, &vals[0], &miner.pubkey());
    let initiated_at = now_ts(&svm);
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");

    set_clock(&mut svm, initiated_at + TIMEOUT_SECS + 1);
    let coll_before = miner_state(&svm, &miner.pubkey()).collateral;
    let user_before = lamports(&svm, &LOTTERY_USER);
    let vault_before = lamports(&svm, &collateral_vault_pda(&miner.pubkey()));

    send(&mut svm, timeout_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, "srctx1"), &vals[0].pubkey(), &vals[0]).expect("t0");
    let logs = send_meta(&mut svm, timeout_ix(&vals[1].pubkey(), &miner.pubkey(), &LOTTERY_USER, "srctx1"), &vals[1].pubkey(), &vals[1]).expect("t1");

    let penalty = required_collateral(SOL_AMOUNT);
    let ms = miner_state(&svm, &miner.pubkey());
    assert_eq!(ms.collateral, coll_before - penalty, "collateral slashed 1.1×");
    assert_eq!(lamports(&svm, &LOTTERY_USER), user_before + penalty, "user refunded the slash");
    assert_eq!(
        lamports(&svm, &collateral_vault_pda(&miner.pubkey())),
        vault_before - penalty,
        "lamports left the miner's vault"
    );
    assert!(!ms.has_active_swap);
    assert_eq!(ms.busy_any_until(), 0, "locally settled ⇒ miner freed immediately, no grace");
    assert_eq!(ms.failed_swaps, 1);

    let ev = timed_out_event(&logs);
    assert_eq!(ev.collateral_chain, "sol");
    assert_eq!(ev.slash, penalty, "slash = what actually moved");
    assert_eq!(ev.penalty, penalty, "absolute penalty figure");
    assert_eq!(ev.reimbursement, penalty, "all of it reached the user");
    assert_eq!(ev.collateral_amount, SOL_AMOUNT);
    assert!(ev.payee.is_empty(), "settled here ⇒ no off-chain payee; the refund went to swap.user");
}

#[test]
fn test_non_sol_backed_timeout_is_verdict_only_and_holds_the_miner() {
    // The split: Solana reaches the verdict and closes the swap, but moves no lamports — the seizure and
    // the user's reimbursement are a separate quorum on the chain holding the bond. The miner stays busy
    // for the settlement grace so nothing can open against a bond that still owes this penalty.
    let (mut svm, _admin, vals, miner) = setup();
    reserve_spoke(&mut svm, &vals[0], &miner.pubkey());
    let initiated_at = now_ts(&svm);
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");
    set_swap_backing(&mut svm, &swap_key("srctx1"), "tao");

    let timeout_ts = initiated_at + TIMEOUT_SECS + 1;
    set_clock(&mut svm, timeout_ts);
    let coll_before = miner_state(&svm, &miner.pubkey()).collateral;
    let user_before = lamports(&svm, &LOTTERY_USER);
    let vault_before = lamports(&svm, &collateral_vault_pda(&miner.pubkey()));

    send(&mut svm, timeout_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, "srctx1"), &vals[0].pubkey(), &vals[0]).expect("t0");
    let logs = send_meta(&mut svm, timeout_ix(&vals[1].pubkey(), &miner.pubkey(), &LOTTERY_USER, "srctx1"), &vals[1].pubkey(), &vals[1]).expect("t1");

    let ms = miner_state(&svm, &miner.pubkey());
    assert_eq!(ms.collateral, coll_before, "local collateral untouched");
    assert_eq!(lamports(&svm, &LOTTERY_USER), user_before, "no local refund");
    assert_eq!(
        lamports(&svm, &collateral_vault_pda(&miner.pubkey())),
        vault_before,
        "no lamports left the vault"
    );
    assert!(!ms.has_active_swap, "the swap is over");
    assert_eq!(
        ms.busy_any_until(),
        timeout_ts + SETTLEMENT_GRACE_SECS,
        "busy until the backing chain settles"
    );
    assert_eq!(ms.failed_swaps, 1, "the verdict still counts as a failure");
    assert!(svm.get_account(&swap_pda(&swap_key("srctx1"))).is_none(), "swap closed");

    // Absolute figures only — these ARE the vault's vote_slash arguments.
    let ev = timed_out_event(&logs);
    let penalty = required_collateral(SOL_AMOUNT);
    assert_eq!(ev.collateral_chain, "tao");
    assert_eq!(ev.slash, 0, "nothing moved on Solana");
    assert_eq!(ev.penalty, penalty);
    assert_eq!(ev.reimbursement, penalty, "the wronged user is owed the whole penalty");
    // This fixture's backing was written onto a btc→sol swap, so there is no TAO leg to name a payee
    // from — the lookup degrades to empty instead of erroring. The real pairing is the next test.
    assert!(ev.payee.is_empty(), "no backing leg ⇒ no payee, and no revert");
}

#[test]
fn test_the_verdict_names_the_payee_on_the_backing_chain() {
    // W3.1: the reimbursement target travels IN the event, so a validator that never saw the swap
    // live can still relay the seizure. sol→tao backed by TAO ⇒ the user's destination-side address.
    let (mut svm, _admin, vals, miner) = setup();
    draw(&mut svm, &vals[0], &miner.pubkey(), HUB_FROM, HUB_TO);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, SOL_AMOUNT, SOL_AMOUNT as u128, TAO_AMOUNT),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("finalize the hub pair");
    let initiated_at = now_ts(&svm);
    do_initiate(&mut svm, &vals, &miner.pubkey(), "srctx1");
    set_swap_backing(&mut svm, &swap_key("srctx1"), "tao");

    set_clock(&mut svm, initiated_at + TIMEOUT_SECS + 1);
    send(&mut svm, timeout_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, "srctx1"), &vals[0].pubkey(), &vals[0]).expect("t0");
    let logs = send_meta(&mut svm, timeout_ix(&vals[1].pubkey(), &miner.pubkey(), &LOTTERY_USER, "srctx1"), &vals[1].pubkey(), &vals[1]).expect("t1");

    let ev = timed_out_event(&logs);
    assert_eq!(ev.collateral_chain, "tao");
    assert_eq!(ev.payee, "userDstAddr", "TAO is the destination leg, so its address is the payee");
    assert_eq!(ev.reimbursement, required_collateral(SOL_AMOUNT), "figures unchanged by W3.1");
}

#[test]
fn test_settlement_grace_is_admin_tunable() {
    let (mut svm, admin, _vals, _miner) = setup();
    assert_eq!(config_acct(&svm).settlement_grace_secs, SETTLEMENT_GRACE_SECS, "seeded default");
    send(
        &mut svm,
        admin_ix(&admin.pubkey(), allways_swap_manager::instruction::SetSettlementGrace { secs: 600 }.data()),
        &admin.pubkey(),
        &admin,
    )
    .expect("set grace");
    assert_eq!(config_acct(&svm).settlement_grace_secs, 600);
    // Out-of-range values are refused by the same validator the seed satisfies.
    assert!(send(
        &mut svm,
        admin_ix(&admin.pubkey(), allways_swap_manager::instruction::SetSettlementGrace { secs: 1 }.data()),
        &admin.pubkey(),
        &admin,
    )
    .is_err());
}

#[test]
fn test_swap_bounds_are_selected_by_backing_not_converted() {
    // Each backing is bounded in its OWN units: the lamport pair never gates a TAO-backed swap and the
    // rao pair never gates a SOL-backed one. A rate conversion here would be a price oracle in a guard.
    let (mut svm, admin, vals, miner) = setup();
    // A rao floor far above the SOL-denominated swap size, and a lamport floor far above the TAO one.
    send(
        &mut svm,
        admin_ix(&admin.pubkey(), allways_swap_manager::instruction::SetTaoMinSwapAmount { amount: TAO_AMOUNT as u64 * 2 }.data()),
        &admin.pubkey(),
        &admin,
    )
    .expect("set tao min");
    let cfg = config_acct(&svm);
    assert_eq!(cfg.min_swap_amount, 1000, "the lamport floor is untouched by the rao setter");
    assert_eq!(cfg.tao_min_swap_amount, TAO_AMOUNT as u64 * 2);

    // SOL-backed fill of the SAME hub↔hub pair passes: it is measured against the lamport bounds.
    draw(&mut svm, &vals[0], &miner.pubkey(), HUB_FROM, HUB_TO);
    send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, SOL_AMOUNT, SOL_AMOUNT as u128, TAO_AMOUNT),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect("sol-backed fill uses the lamport bounds");

    // Same pair, TAO-backed, TAO leg below the rao floor → AmountBelowMin (never converted).
    let (mut svm, admin, vals, miner) = setup();
    send(
        &mut svm,
        admin_ix(&admin.pubkey(), allways_swap_manager::instruction::SetTaoMinSwapAmount { amount: TAO_AMOUNT as u64 * 2 }.data()),
        &admin.pubkey(),
        &admin,
    )
    .expect("set tao min");
    draw(&mut svm, &vals[0], &miner.pubkey(), HUB_FROM, HUB_TO);
    set_reservation_backing(&mut svm, &miner.pubkey(), "tao");
    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("tao leg below the rao floor");
    assert!(err.contains("AmountBelowMin"), "rao bounds applied to a tao backing: {err}");

    // Lower the rao floor and the same fill clears bounds — failing later, at the W2 entry fuse (this
    // setup never beat a heartbeat), which proves bounds were the only thing standing in the way.
    send(
        &mut svm,
        admin_ix(
            &admin.pubkey(),
            allways_swap_manager::instruction::SetTaoMinSwapAmount { amount: TAO_MIN_SWAP_AMOUNT_RAO }.data(),
        ),
        &admin.pubkey(),
        &admin,
    )
    .expect("lower tao min");
    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("the fuse still guards the tao purse");
    assert!(err.contains("AttestationStale"), "bounds cleared, the fuse did not: {err}");
}

#[test]
fn test_collateral_binds_to_the_backing_leg_on_either_side_of_the_pair() {
    // Leg lookup, not pair branching: on sol→tao the SOL-backed amount is the SOURCE leg. Naming the
    // destination (TAO) leg as the collateral amount must be rejected.
    let (mut svm, _admin, vals, miner) = setup();
    draw(&mut svm, &vals[0], &miner.pubkey(), HUB_FROM, HUB_TO);
    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, TAO_AMOUNT as u64, SOL_AMOUNT as u128, TAO_AMOUNT),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("collateral_amount must equal the sol leg");
    assert!(err.contains("InvalidAmount"), "{err}");

    // A backing that is on neither leg has nothing to size against. The named amount sits inside the
    // rao bounds so the leg lookup — not the size check that precedes it — is what rejects the fill.
    let (mut svm, _admin, vals, miner) = setup();
    draw(&mut svm, &vals[0], &miner.pubkey(), SPOKE_FROM, SPOKE_TO);
    set_reservation_backing(&mut svm, &miner.pubkey(), "tao"); // btc→sol, backed by TAO
    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, TAO_AMOUNT as u64, OTHER_AMOUNT, SOL_AMOUNT as u128),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("backing must be one of the legs");
    assert!(err.contains("BackingNotInLegs"), "{err}");
}

#[test]
fn test_unsupported_backing_is_refused_at_finalize_and_at_initiate() {
    // A chain with no purse (no local vault, no attestation) is refused outright rather than falling
    // through to the local vault. "btc" is on both legs here, so only the purse lookup can reject it.
    let (mut svm, _admin, vals, miner) = setup();
    draw(&mut svm, &vals[0], &miner.pubkey(), SPOKE_FROM, SPOKE_TO);
    set_reservation_backing(&mut svm, &miner.pubkey(), "btc");
    let err = send(
        &mut svm,
        finalize_ix(&vals[0].pubkey(), &miner.pubkey(), &LOTTERY_USER, OTHER_AMOUNT as u64, OTHER_AMOUNT, SOL_AMOUNT as u128),
        &vals[0].pubkey(),
        &vals[0],
    )
    .expect_err("finalize must refuse an unsupported backing");
    assert!(err.contains("BackingNotSupported"), "{err}");

    // Same at the obligation gate: a claim whose backing flipped after the fill can't be attested.
    let (mut svm, _admin, vals, miner) = setup();
    reserve_spoke(&mut svm, &vals[0], &miner.pubkey());
    send(&mut svm, claim_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0]).expect("claim");
    set_swap_backing(&mut svm, &swap_key("srctx1"), "btc");
    let err = send(&mut svm, initiate_ix(&vals[0].pubkey(), &miner.pubkey(), "srctx1"), &vals[0].pubkey(), &vals[0])
        .expect_err("initiate must refuse an unsupported backing");
    assert!(err.contains("BackingNotSupported"), "{err}");
}

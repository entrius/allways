// The Base USDC hub: fail-closed until configured AND beaten, a fuse per hub, the EVM bond-address
// binding, and the bond key a Base-backed swap pins at initiate (LiteSVM).
//   cargo test -p allways_swap_manager --test test_base_hub
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::clock::Clock, solana_program::instruction::Instruction,
        AccountDeserialize, AccountSerialize, AnchorDeserialize, Discriminator, InstructionData,
        ToAccountMetas,
    },
    allways_swap_manager::constants::{
        required_collateral, ATTEST_MAX_AGE_SECS, BACKING_BIT_BASEUSDC, BACKING_BIT_SOL, POOL_WINDOW_SECS,
    },
    allways_swap_manager::events::{AttestHeartbeat, EvmAddressBound, SwapTimedOut},
    allways_swap_manager::instructions::bind_evm_address::{evm_bind_digest, evm_bind_message},
    allways_swap_manager::state::{Config, EvmAddressBinding, EvmBinding, MinerState, Pool, Reservation, Swap},
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

const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;
const SLOT_HASHES_ID: Pubkey = Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111");
const REQ_ACTIVATE: u8 = 0;
const REQ_INITIATE: u8 = 2;
const REQ_TIMEOUT: u8 = 7;
const REQ_SET_ATTESTATION: u8 = 9;
const REQ_ATTEST_HEARTBEAT: u8 = 10;
const BASE_TS: i64 = 1_700_000_000;
const TTL: i64 = 1_800;
const TIMEOUT_SECS: i64 = 3_600;
const MIN_COLLATERAL: u64 = 1_000_000_000; // 1 SOL
const COLLATERAL: u64 = 10_000_000_000; // 10 SOL
const LOTTERY_USER: Pubkey = Pubkey::new_from_array([7u8; 32]);
const FROM_TX_BLOCK: u32 = 800_000;
const SOL: &str = "sol";
const TAO: &str = "tao";
const BASE: &str = "baseusdc";
const MINER_FROM: &str = "minerSrcAddr";
const MINER_TO: &str = "minerDstAddr";
const RATE: u128 = 1_500_000_000_000_000_000;

// The Base hub's opening policy in µUSDC (6 dp): 1 USDC min swap, 25 USDC bond floor.
const USDC_MIN_SWAP: u64 = 1_000_000;
const USDC_MIN_COLLATERAL: u64 = 25_000_000;
const USDC_SWAP: u64 = 5_000_000; // 5 USDC — the baseusdc leg of a sol→baseusdc swap
const SOL_LEG: u128 = 30_000_000;
// anvil's account #0 (0xf39F…2266): the bind vector's key, shared with tests/test_solana_evm_bind.py.
const EVM_KEY: [u8; 32] = [
    0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3, 0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff, 0x94, //
    0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfc, 0xae, 0x78, 0x4d, 0x7b, 0xf4, 0xf2, 0xff, 0x80,
];
const OTHER_EVM_KEY: [u8; 32] = [0x42; 32];

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
    Pubkey::find_program_address(&[b"vote", &[REQ_SET_ATTESTATION], m.as_ref(), chain.as_bytes()], &pid()).0
}
fn heartbeat_round_pda(hub: &str) -> Pubkey {
    Pubkey::find_program_address(
        &[b"vote", &[REQ_ATTEST_HEARTBEAT], config_pda().as_ref(), hub.as_bytes()],
        &pid(),
    )
    .0
}
fn resv_pda(m: &Pubkey, backing: &str) -> Pubkey {
    Pubkey::find_program_address(&[b"resv", m.as_ref(), backing.as_bytes()], &pid()).0
}
fn quote_pda(m: &Pubkey, f: &str, t: &str, b: &str) -> Pubkey {
    Pubkey::find_program_address(&[b"quote", m.as_ref(), f.as_bytes(), t.as_bytes(), b.as_bytes()], &pid()).0
}
fn pool_pda(m: &Pubkey, backing: &str) -> Pubkey {
    Pubkey::find_program_address(&[b"pool", m.as_ref(), backing.as_bytes()], &pid()).0
}
fn swap_pda(key: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"swap", key.as_ref()], &pid()).0
}
fn bind_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"bind", m.as_ref()], &pid()).0
}
fn evm_bind_pda(m: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"evmbind", m.as_ref()], &pid()).0
}
fn evm_addr_pda(addr: &[u8; 20]) -> Pubkey {
    Pubkey::find_program_address(&[b"evmaddr", addr.as_ref()], &pid()).0
}
fn srclock_pda(_m: &Pubkey, from_chain: &str) -> Pubkey {
    Pubkey::find_program_address(
        &[b"srclock", from_chain.as_bytes(), &allways_swap_manager::source_lock_key(MINER_FROM, "userSrcAddr")],
        &pid(),
    )
    .0
}
fn addr_lock_pda(addr: &str) -> Pubkey {
    Pubkey::find_program_address(&[b"addrlock", &allways_swap_manager::addr_lock_key(addr)], &pid()).0
}
/// The payout wallet's lock, or None when it is the deposit wallet's (same lock).
fn to_addr_lock_pda(from: &str, to: &str) -> Option<Pubkey> {
    (from.to_ascii_lowercase() != to.to_ascii_lowercase()).then(|| addr_lock_pda(to))
}
fn swap_key(from_tx_hash: &str) -> [u8; 32] {
    hashv(&[from_tx_hash.as_bytes()]).to_bytes()
}
fn padded(addr: &[u8; 20]) -> [u8; 32] {
    let mut k = [0u8; 32];
    k[12..].copy_from_slice(addr);
    k
}

fn set_clock(svm: &mut LiteSVM, ts: i64) {
    let mut clock = svm.get_sysvar::<Clock>();
    clock.unix_timestamp = ts;
    svm.set_sysvar::<Clock>(&clock);
}
fn now_ts(svm: &LiteSVM) -> i64 {
    svm.get_sysvar::<Clock>().unix_timestamp
}
fn send_logs(svm: &mut LiteSVM, ix: Instruction, signer: &Keypair) -> Result<Vec<String>, String> {
    svm.expire_blockhash();
    let bh = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(&signer.pubkey()), &bh);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|m| m.logs).map_err(|e| format!("{:?}", e))
}
fn send(svm: &mut LiteSVM, ix: Instruction, signer: &Keypair) -> Result<(), String> {
    send_logs(svm, ix, signer).map(|_| ())
}
fn b64(s: &str) -> Vec<u8> {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let (mut acc, mut bits, mut out) = (0u32, 0u32, Vec::new());
    for c in s.bytes().filter(|&c| c != b'=') {
        acc = (acc << 6) | A.iter().position(|&a| a == c).expect("base64 alphabet") as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}
fn event<E: Discriminator + AnchorDeserialize>(logs: &[String]) -> E {
    for l in logs {
        if let Some(raw) = l.strip_prefix("Program data: ").map(b64) {
            if raw.len() > 8 && raw[..8] == E::DISCRIMINATOR[..] {
                return E::deserialize(&mut &raw[8..]).unwrap();
            }
        }
    }
    panic!("event not in logs: {logs:?}");
}

fn config_acct(svm: &LiteSVM) -> Config {
    Config::try_deserialize(&mut svm.get_account(&config_pda()).unwrap().data.as_slice()).unwrap()
}
fn miner_state(svm: &LiteSVM, m: &Pubkey) -> MinerState {
    MinerState::try_deserialize(&mut svm.get_account(&miner_pda(m)).unwrap().data.as_slice()).unwrap()
}
fn swap_acct(svm: &LiteSVM, key: &[u8; 32]) -> Swap {
    Swap::try_deserialize(&mut svm.get_account(&swap_pda(key)).unwrap().data.as_slice()).unwrap()
}
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
        allways_swap_manager::accounts::AdminConfig { admin: *admin, config: config_pda() }.to_account_metas(None),
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
            collateral_vault: collateral_vault_pda(miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn quote_ix(miner: &Pubkey, f: &str, t: &str, b: &str) -> Instruction {
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
            from_addr_lock_key: allways_swap_manager::addr_lock_key(MINER_FROM),
            to_addr_lock_key: allways_swap_manager::addr_lock_key(MINER_TO),
        }
        .data(),
        allways_swap_manager::accounts::SetQuote {
            miner: *miner,
            miner_state: miner_pda(miner),
            quote: quote_pda(miner, f, t, b),
            from_addr_lock: addr_lock_pda(MINER_FROM),
            to_addr_lock: to_addr_lock_pda(MINER_FROM, MINER_TO),
            treasury: treasury_pda(),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn activate_ix(validator: &Pubkey, miner: &Pubkey, backing: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteActivate { backing: backing.to_string() }.data(),
        allways_swap_manager::accounts::VoteActivate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            vote_round: vote_pda(REQ_ACTIVATE, miner.as_ref()),
            attestation: (backing != SOL).then(|| attest_pda(miner, backing)),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn attest_ix(validator: &Pubkey, miner: &Pubkey, chain: &str, bal: u64, locked: bool, epoch: u64) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteSetAttestation {
            chain: chain.to_string(),
            effective_balance: bal,
            locked,
            epoch,
        }
        .data(),
        allways_swap_manager::accounts::VoteSetAttestation {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner, chain),
            attestation: attest_pda(miner, chain),
            vote_round: attest_round_pda(miner, chain),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn heartbeat_ix(validator: &Pubkey, hub: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteAttestHeartbeat { hub: hub.to_string() }.data(),
        allways_swap_manager::accounts::VoteAttestHeartbeat {
            validator: *validator,
            config: config_pda(),
            vote_round: heartbeat_round_pda(hub),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn open_ix(router: &Pubkey, miner: &Pubkey, f: &str, t: &str, b: &str) -> Instruction {
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
            attestation: (b != SOL).then(|| attest_pda(miner, b)),
            pool: pool_pda(miner, b),
            treasury: treasury_pda(),
            reservation: resv_pda(miner, b),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn resolve_ix(caller: &Pubkey, miner: &Pubkey, backing: &str) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::ResolvePool {}.data(),
        allways_swap_manager::accounts::ResolvePool {
            caller: *caller,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            pool: pool_pda(miner, backing),
            reservation: resv_pda(miner, backing),
            slot_hashes: SLOT_HASHES_ID,
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
#[allow(clippy::too_many_arguments)]
fn finalize_ix(
    backing: &str,
    from_chain: &str,
    router: &Pubkey,
    miner: &Pubkey,
    collateral_amount: u64,
    from_amount: u128,
    to_amount: u128,
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
            source_lock_key: allways_swap_manager::source_lock_key(MINER_FROM, "userSrcAddr"),
        }
        .data(),
        allways_swap_manager::accounts::FinalizeReservation {
            router: *router,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner, backing),
            attestation: (backing != SOL).then(|| attest_pda(miner, backing)),
            source_lock: srclock_pda(miner, from_chain),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn claim_ix(backing: &str, caller: &Pubkey, miner: &Pubkey, tx: &str) -> Instruction {
    let key = swap_key(tx);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::SubmitSwapClaim {
            swap_key: key,
            from_tx_hash: tx.to_string(),
            from_tx_block: FROM_TX_BLOCK,
        }
        .data(),
        allways_swap_manager::accounts::SubmitSwapClaim {
            caller: *caller,
            config: config_pda(),
            miner: *miner,
            reservation: resv_pda(miner, backing),
            swap: swap_pda(&key),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn initiate_ix(backing: &str, from_chain: &str, validator: &Pubkey, miner: &Pubkey, tx: &str) -> Instruction {
    initiate_with(backing, from_chain, validator, miner, tx, (backing != SOL).then(|| attest_pda(miner, backing)))
}
fn initiate_with(
    backing: &str,
    from_chain: &str,
    validator: &Pubkey,
    miner: &Pubkey,
    tx: &str,
    attestation: Option<Pubkey>,
) -> Instruction {
    let key = swap_key(tx);
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::VoteInitiate {
            swap_key: key,
            source_lock_key: allways_swap_manager::source_lock_key(MINER_FROM, "userSrcAddr"),
        }
        .data(),
        allways_swap_manager::accounts::VoteInitiate {
            validator: *validator,
            config: config_pda(),
            miner: *miner,
            miner_state: miner_pda(miner),
            reservation: resv_pda(miner, backing),
            source_lock: srclock_pda(miner, from_chain),
            vote_round: vote_pda(REQ_INITIATE, &key),
            swap: swap_pda(&key),
            attestation,
            binding: bind_pda(miner),
            evm_binding: evm_bind_pda(miner),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn timeout_ix(validator: &Pubkey, miner: &Pubkey, tx: &str) -> Instruction {
    let key = swap_key(tx);
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
fn evm_addr(key: &[u8; 32]) -> [u8; 20] {
    let pk = libsecp256k1::PublicKey::from_secret_key(&libsecp256k1::SecretKey::parse(key).unwrap()).serialize();
    hashv(&[&pk[1..]]).to_bytes()[12..].try_into().unwrap()
}
/// personal_sign by `key` over the bind message naming `addr` and `miner`, as r || s || v (v = 27/28).
fn evm_sign(key: &[u8; 32], addr: &[u8; 20], miner: &Pubkey) -> [u8; 65] {
    let digest = evm_bind_digest(&evm_bind_message(addr, miner, &pid()));
    let sk = libsecp256k1::SecretKey::parse(key).unwrap();
    let (sig, recid) = libsecp256k1::sign(&libsecp256k1::Message::parse(&digest), &sk);
    let mut out = [0u8; 65];
    out[..64].copy_from_slice(&sig.serialize());
    out[64] = recid.serialize() + 27;
    out
}
fn bind_evm_ix(miner: &Pubkey, addr: [u8; 20], evm_signature: [u8; 65]) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::BindEvmAddress { evm_address: addr, evm_signature }.data(),
        allways_swap_manager::accounts::BindEvmAddress {
            miner: *miner,
            evm_binding: evm_bind_pda(miner),
            evm_address_binding: evm_addr_pda(&addr),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}

/// `signer` binds the address of `key`, signed by that key.
fn bind_evm(svm: &mut LiteSVM, signer: &Keypair, key: &[u8; 32]) -> Result<Vec<String>, String> {
    let addr = evm_addr(key);
    send_logs(svm, bind_evm_ix(&signer.pubkey(), addr, evm_sign(key, &addr, &signer.pubkey())), signer)
}

/// init + 3 validators + a SOL-active miner. The Base hub is exactly as a fresh upgrade leaves it.
fn setup() -> (LiteSVM, Keypair, Vec<Keypair>, Keypair) {
    let mut svm = LiteSVM::new();
    svm.add_program(pid(), include_bytes!("../../../target/deploy/allways_swap_manager.so")).unwrap();
    set_clock(&mut svm, BASE_TS);
    let admin = Keypair::new();
    svm.airdrop(&admin.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, init_ix(&admin.pubkey()), &admin).expect("init");
    let mut vals = Vec::new();
    for _ in 0..3 {
        let v = Keypair::new();
        svm.airdrop(&v.pubkey(), 100_000_000_000).unwrap();
        let ix = admin_ix(&admin.pubkey(), allways_swap_manager::instruction::AddValidator { validator: v.pubkey(), weight: 1 }.data());
        send(&mut svm, ix, &admin).expect("add val");
        vals.push(v);
    }
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 100_000_000_000).unwrap();
    send(&mut svm, post_ix(&miner.pubkey(), COLLATERAL), &miner).expect("post");
    for v in &vals[..2] {
        send(&mut svm, activate_ix(&v.pubkey(), &miner.pubkey(), SOL), v).expect("activate sol");
    }
    (svm, admin, vals, miner)
}

fn quorum(svm: &mut LiteSVM, vals: &[Keypair], ix: impl Fn(&Pubkey) -> Instruction) -> Vec<String> {
    send(svm, ix(&vals[0].pubkey()), &vals[0]).expect("vote 0");
    send_logs(svm, ix(&vals[1].pubkey()), &vals[1]).expect("vote 1")
}
fn beat(svm: &mut LiteSVM, vals: &[Keypair], hub: &str) -> Vec<String> {
    quorum(svm, vals, |v| heartbeat_ix(v, hub))
}
fn attest(svm: &mut LiteSVM, vals: &[Keypair], m: &Pubkey, chain: &str, bal: u64) {
    quorum(svm, vals, |v| attest_ix(v, m, chain, bal, true, 1));
}
/// What the admin runs to open the Base hub: bounds, bond floor and a max age for its fuse.
fn open_base_policy(svm: &mut LiteSVM, admin: &Keypair) {
    use allways_swap_manager::instruction as ix;
    for data in [
        ix::SetUsdcMinSwapAmount { amount: USDC_MIN_SWAP }.data(),
        ix::SetUsdcMinCollateral { amount: USDC_MIN_COLLATERAL }.data(),
        ix::SetAttestMaxAge { hub: BASE.to_string(), secs: ATTEST_MAX_AGE_SECS }.data(),
    ] {
        send(svm, admin_ix(&admin.pubkey(), data), admin).expect("open base policy");
    }
}
/// Give the miner the baseusdc bit directly — the backdoor the fail-closed test needs, since the real
/// door (vote_activate) is exactly what stays shut.
fn force_base_bit(svm: &mut LiteSVM, m: &Pubkey) {
    let mut ms = miner_state(svm, m);
    ms.set_backing(BACKING_BIT_BASEUSDC, true);
    let mut buf = Vec::new();
    ms.try_serialize(&mut buf).unwrap();
    overwrite(svm, miner_pda(m), buf);
}
fn draw(svm: &mut LiteSVM, val: &Keypair, m: &Pubkey, f: &str, t: &str, b: &str) {
    let now = now_ts(svm);
    send(svm, open_ix(&val.pubkey(), m, f, t, b), val).expect("open");
    set_clock(svm, now + POOL_WINDOW_SECS + 1);
    send(svm, resolve_ix(&val.pubkey(), m, b), val).expect("arm draw");
    let a = svm.get_account(&pool_pda(m, b)).unwrap();
    let seed_slot = Pool::try_deserialize(&mut a.data.as_slice()).unwrap().seed_slot;
    let entries: Vec<(u64, Hash)> =
        [seed_slot - 1, seed_slot, seed_slot + 1].iter().map(|&s| (s, Hash::new_from_array([s as u8; 32]))).collect();
    svm.set_sysvar::<SlotHashes>(&SlotHashes::new(&entries));
    send(svm, resolve_ix(&val.pubkey(), m, b), val).expect("resolve");
}
/// Move a drawn reservation to another backing's PDA, as `test_backing.rs` does.
fn rehome_reservation(svm: &mut LiteSVM, m: &Pubkey, from: &str, to: &str) {
    let old = svm.get_account(&resv_pda(m, from)).unwrap();
    let mut r = Reservation::try_deserialize(&mut old.data.as_slice()).unwrap();
    r.collateral_chain = to.to_string();
    r.bump = Pubkey::find_program_address(&[b"resv", m.as_ref(), to.as_bytes()], &pid()).1;
    let mut data = Vec::new();
    r.try_serialize(&mut data).unwrap();
    data.resize(old.data.len(), 0);
    svm.set_account(resv_pda(m, to), Account { data, ..old }).unwrap();
    svm.set_account(resv_pda(m, from), Account::default()).unwrap();
}

// --- fail closed ------------------------------------------------------------------------------

#[test]
fn test_a_fresh_base_hub_refuses_reserve_and_finalize() {
    let (mut svm, admin, vals, miner) = setup();
    let m = miner.pubkey();
    let v = &vals[0];
    let cfg = config_acct(&svm);
    assert_eq!((cfg.usdc_min_swap_amount, cfg.usdc_max_swap_amount, cfg.usdc_min_collateral), (0, 0, 0));
    assert_eq!((cfg.usdc_last_attest_heartbeat, cfg.usdc_attest_max_age_secs), (0, 0));

    // A locked, ample attestation exists — so only the hub's own closure can refuse what follows.
    attest(&mut svm, &vals, &m, BASE, 100 * USDC_MIN_COLLATERAL);

    // Reserve: even a miner already holding the bit can't open a Base-backed pool.
    force_base_bit(&mut svm, &m);
    send(&mut svm, quote_ix(&m, SOL, BASE, BASE), &miner).expect("quote");
    let err = send(&mut svm, open_ix(&v.pubkey(), &m, SOL, BASE, BASE), v).unwrap_err();
    assert!(err.contains("BackingNotConfigured"), "{err}");

    // Finalize: a drawn reservation re-pinned to the Base backing is refused at the fill.
    send(&mut svm, quote_ix(&m, SOL, BASE, SOL), &miner).expect("sol-backed quote");
    draw(&mut svm, v, &m, SOL, BASE, SOL);
    rehome_reservation(&mut svm, &m, SOL, BASE);
    let fill = finalize_ix(BASE, SOL, &v.pubkey(), &m, USDC_SWAP, SOL_LEG, USDC_SWAP as u128);
    let err = send(&mut svm, fill.clone(), v).unwrap_err();
    assert!(err.contains("BackingNotConfigured"), "{err}");

    // The admin opening the policy is not enough: no Base heartbeat has ever landed.
    open_base_policy(&mut svm, &admin);
    let err = send(&mut svm, fill, v).unwrap_err();
    assert!(err.contains("AttestationStale"), "{err}");
    let err = send(&mut svm, open_ix(&v.pubkey(), &m, SOL, BASE, BASE), v).unwrap_err();
    assert!(err.contains("AttestationStale"), "{err}");
}

#[test]
fn test_base_activation_needs_the_admin_policy_and_then_a_base_heartbeat() {
    let (mut svm, admin, vals, miner) = setup();
    let m = miner.pubkey();
    attest(&mut svm, &vals, &m, BASE, USDC_MIN_COLLATERAL);
    beat(&mut svm, &vals, TAO); // a live TAO relay opens nothing on Base
    let try_activate = |svm: &mut LiteSVM| send(svm, activate_ix(&vals[0].pubkey(), &m, BASE), &vals[0]);

    let err = try_activate(&mut svm).unwrap_err();
    assert!(err.contains("BackingNotConfigured"), "{err}");
    open_base_policy(&mut svm, &admin);
    let err = try_activate(&mut svm).unwrap_err();
    assert!(err.contains("AttestationStale"), "{err}");
    beat(&mut svm, &vals, BASE);
    quorum(&mut svm, &vals, |val| activate_ix(val, &m, BASE));
    assert_ne!(miner_state(&svm, &m).active_backings & BACKING_BIT_BASEUSDC, 0);
}

#[test]
fn test_a_fresh_base_hub_refuses_initiate_on_a_claim_flipped_to_it() {
    let (mut svm, _admin, vals, miner) = setup();
    let m = miner.pubkey();
    let v = &vals[0];
    send(&mut svm, quote_ix(&m, SOL, BASE, SOL), &miner).expect("quote");
    draw(&mut svm, v, &m, SOL, BASE, SOL);
    send(&mut svm, finalize_ix(SOL, SOL, &v.pubkey(), &m, SOL_LEG as u64, SOL_LEG, USDC_SWAP as u128), v)
        .expect("sol-backed fill");
    send(&mut svm, claim_ix(SOL, &v.pubkey(), &m, "tx1"), v).expect("claim");
    // Flip the claim (and its reservation) to the Base backing: the last gate must still hold.
    let key = swap_key("tx1");
    let mut s = swap_acct(&svm, &key);
    s.collateral_chain = BASE.to_string();
    s.collateral_amount = USDC_SWAP;
    let mut buf = Vec::new();
    s.try_serialize(&mut buf).unwrap();
    overwrite(&mut svm, swap_pda(&key), buf);
    rehome_reservation(&mut svm, &m, SOL, BASE);
    let err = send(&mut svm, initiate_with(BASE, SOL, &v.pubkey(), &m, "tx1", None), v).unwrap_err();
    assert!(err.contains("BackingNotConfigured"), "{err}");
}

// --- one fuse per hub ---------------------------------------------------------------------------

#[test]
fn test_each_hub_beats_its_own_round_and_field() {
    let (mut svm, _admin, vals, _miner) = setup();
    set_clock(&mut svm, BASE_TS + 100);
    let logs = beat(&mut svm, &vals, BASE);
    let ev: AttestHeartbeat = event(&logs);
    assert_eq!((ev.at, ev.hub.as_str()), (BASE_TS + 100, BASE));
    let cfg = config_acct(&svm);
    assert_eq!(cfg.usdc_last_attest_heartbeat, BASE_TS + 100);
    assert_eq!(cfg.last_attest_heartbeat, 0, "a Base beat leaves TAO's fuse alone");

    // A half-cast TAO round is not disturbed by a Base round running beside it.
    send(&mut svm, heartbeat_ix(&vals[0].pubkey(), TAO), &vals[0]).expect("tao vote 0");
    set_clock(&mut svm, BASE_TS + 200);
    beat(&mut svm, &vals, BASE);
    send(&mut svm, heartbeat_ix(&vals[1].pubkey(), TAO), &vals[1]).expect("tao vote 1");
    let cfg = config_acct(&svm);
    assert_eq!(cfg.last_attest_heartbeat, BASE_TS + 200);
    assert_eq!(cfg.usdc_last_attest_heartbeat, BASE_TS + 200);
}

#[test]
fn test_sol_and_unknown_hubs_have_no_heartbeat() {
    let (mut svm, _admin, vals, _miner) = setup();
    let err = send(&mut svm, heartbeat_ix(&vals[0].pubkey(), SOL), &vals[0]).unwrap_err();
    assert!(err.contains("BackingSettlesLocally"), "{err}");
    let err = send(&mut svm, heartbeat_ix(&vals[0].pubkey(), "arbusdc"), &vals[0]).unwrap_err();
    assert!(err.contains("BackingNotSupported"), "{err}");
}

#[test]
fn test_a_stale_base_fuse_leaves_tao_and_sol_entry_open() {
    let (mut svm, admin, vals, miner) = setup();
    let m = miner.pubkey();
    let v = &vals[0];
    open_base_policy(&mut svm, &admin);
    beat(&mut svm, &vals, TAO);
    attest(&mut svm, &vals, &m, TAO, 250_000_000);
    quorum(&mut svm, &vals, |val| activate_ix(val, &m, TAO));
    send(&mut svm, quote_ix(&m, SOL, TAO, TAO), &miner).expect("tao quote");
    send(&mut svm, quote_ix(&m, "btc", SOL, SOL), &miner).expect("sol quote");
    assert_eq!(config_acct(&svm).usdc_last_attest_heartbeat, 0, "Base never beaten");

    send(&mut svm, open_ix(&v.pubkey(), &m, SOL, TAO, TAO), v).expect("TAO entry ignores the Base fuse");
    send(&mut svm, open_ix(&v.pubkey(), &m, "btc", SOL, SOL), v).expect("SOL entry ignores every fuse");
}

#[test]
fn test_a_stale_tao_fuse_leaves_base_entry_open() {
    let (mut svm, admin, vals, miner) = setup();
    let m = miner.pubkey();
    open_base_policy(&mut svm, &admin);
    beat(&mut svm, &vals, BASE);
    attest(&mut svm, &vals, &m, BASE, USDC_MIN_COLLATERAL);
    quorum(&mut svm, &vals, |val| activate_ix(val, &m, BASE));
    send(&mut svm, quote_ix(&m, SOL, BASE, BASE), &miner).expect("base quote");
    assert_eq!(config_acct(&svm).last_attest_heartbeat, 0, "TAO never beaten");
    send(&mut svm, open_ix(&vals[0].pubkey(), &m, SOL, BASE, BASE), &vals[0]).expect("Base entry ignores the TAO fuse");
}

// --- admin per-hub setters ----------------------------------------------------------------------

#[test]
fn test_per_hub_setters_touch_only_their_hub_and_refuse_sol() {
    use allways_swap_manager::instruction as ix;
    let (mut svm, admin, _vals, _miner) = setup();
    let a = admin.pubkey();
    let before = config_acct(&svm);

    send(&mut svm, admin_ix(&a, ix::SetAttestMaxAge { hub: BASE.to_string(), secs: 7_200 }.data()), &admin).unwrap();
    send(&mut svm, admin_ix(&a, ix::BumpVaultGeneration { hub: BASE.to_string() }.data()), &admin).unwrap();
    let cfg = config_acct(&svm);
    assert_eq!(cfg.usdc_attest_max_age_secs, 7_200);
    assert_eq!(cfg.usdc_vault_generation, 1);
    assert_eq!(cfg.attest_max_age_secs, before.attest_max_age_secs, "TAO untouched");
    assert_eq!(cfg.vault_generation, 0, "TAO untouched");

    for data in [
        ix::SetAttestMaxAge { hub: SOL.to_string(), secs: 7_200 }.data(),
        ix::BumpVaultGeneration { hub: SOL.to_string() }.data(),
    ] {
        let err = send(&mut svm, admin_ix(&a, data), &admin).unwrap_err();
        assert!(err.contains("BackingSettlesLocally"), "{err}");
    }
    // Same bounds as every other hub: the age window, the dust floor, non-contradictory bounds.
    for data in [
        ix::SetAttestMaxAge { hub: BASE.to_string(), secs: 60 }.data(),
        ix::SetUsdcMinSwapAmount { amount: 999 }.data(),
        ix::SetUsdcMinCollateral { amount: 0 }.data(),
    ] {
        assert!(send(&mut svm, admin_ix(&a, data), &admin).is_err());
    }
    send(&mut svm, admin_ix(&a, ix::SetUsdcMinSwapAmount { amount: 2_000_000 }.data()), &admin).unwrap();
    assert!(send(&mut svm, admin_ix(&a, ix::SetUsdcMaxSwapAmount { amount: 1_000_000 }.data()), &admin).is_err());
    send(&mut svm, admin_ix(&a, ix::SetUsdcMaxSwapAmount { amount: 50_000_000 }.data()), &admin).unwrap();
    let cfg = config_acct(&svm);
    assert_eq!((cfg.usdc_min_swap_amount, cfg.usdc_max_swap_amount), (2_000_000, 50_000_000));
}

// --- EVM bond-address binding --------------------------------------------------------------------

#[test]
fn test_evm_binding_is_set_once_in_both_directions() {
    let (mut svm, _admin, _vals, miner) = setup();
    let m = miner.pubkey();
    let addr = evm_addr(&EVM_KEY);
    let logs = bind_evm(&mut svm, &miner, &EVM_KEY).expect("bind");
    let ev: EvmAddressBound = event(&logs);
    assert_eq!((ev.miner, ev.evm_address), (m, addr));
    let b = EvmBinding::try_deserialize(&mut svm.get_account(&evm_bind_pda(&m)).unwrap().data.as_slice()).unwrap();
    assert_eq!((b.miner, b.evm_address), (m, addr));
    let r = EvmAddressBinding::try_deserialize(&mut svm.get_account(&evm_addr_pda(&addr)).unwrap().data.as_slice())
        .unwrap();
    assert_eq!(r.miner, m);

    // Same address again: a harmless no-op. A different one, properly signed: refused (V-M1).
    bind_evm(&mut svm, &miner, &EVM_KEY).expect("idempotent rebind");
    let err = bind_evm(&mut svm, &miner, &OTHER_EVM_KEY).unwrap_err();
    assert!(err.contains("EvmAddressChangeForbidden"), "{err}");
    let other_addr = evm_addr(&OTHER_EVM_KEY);
    assert!(svm.get_account(&evm_addr_pda(&other_addr)).is_none_or(|a| a.data.is_empty()), "no marker leaked");

    // Another pubkey holding the same EVM key still can't claim an address already bound.
    let other = Keypair::new();
    svm.airdrop(&other.pubkey(), 100_000_000_000).unwrap();
    let err = bind_evm(&mut svm, &other, &EVM_KEY).unwrap_err();
    assert!(err.contains("EvmAddressAlreadyBound"), "{err}");
}

#[test]
fn test_evm_binding_needs_no_collateral_and_validators_sign_too() {
    let (mut svm, _admin, vals, _miner) = setup();
    let stranger = Keypair::new();
    svm.airdrop(&stranger.pubkey(), 100_000_000_000).unwrap();
    assert!(svm.get_account(&miner_pda(&stranger.pubkey())).is_none(), "no MinerState, no collateral");
    bind_evm(&mut svm, &stranger, &EVM_KEY).expect("the EVM signature alone is enough");
    // A validator takes the same path: an unsigned claim is refused for it too.
    let addr = evm_addr(&OTHER_EVM_KEY);
    let err = send(&mut svm, bind_evm_ix(&vals[2].pubkey(), addr, [0u8; 65]), &vals[2]).unwrap_err();
    assert!(err.contains("EvmSignatureMalformed"), "{err}");
    bind_evm(&mut svm, &vals[2], &OTHER_EVM_KEY).expect("validator binds");
}

#[test]
fn test_evm_binding_refuses_a_signature_that_does_not_prove_the_address() {
    let (mut svm, _admin, _vals, miner) = setup();
    let m = miner.pubkey();
    let addr = evm_addr(&EVM_KEY);
    // Signed by another EVM key: the recovered address isn't the one being bound.
    let err = send(&mut svm, bind_evm_ix(&m, addr, evm_sign(&OTHER_EVM_KEY, &addr, &m)), &miner).unwrap_err();
    assert!(err.contains("EvmSignatureMismatch"), "{err}");
    // The right key, but naming another Solana pubkey: it can't be replayed onto this signer.
    let err = send(&mut svm, bind_evm_ix(&m, addr, evm_sign(&EVM_KEY, &addr, &LOTTERY_USER)), &miner).unwrap_err();
    assert!(err.contains("EvmSignatureMismatch"), "{err}");

    let good = evm_sign(&EVM_KEY, &addr, &m);
    // The malleable twin (n - s, flipped v) recovers the same key, but is refused as high-s.
    let mut twin = good;
    let mut s = libsecp256k1::curve::Scalar::default();
    let _ = s.set_b32(&good[32..64].try_into().unwrap());
    twin[32..64].copy_from_slice(&(-s).b32());
    twin[64] = if good[64] == 27 { 28 } else { 27 };
    let err = send(&mut svm, bind_evm_ix(&m, addr, twin), &miner).unwrap_err();
    assert!(err.contains("EvmSignatureMalformed"), "{err}");
    // v in the 0/1 convention is refused: only 27/28 is accepted.
    let mut raw_v = good;
    raw_v[64] -= 27;
    let err = send(&mut svm, bind_evm_ix(&m, addr, raw_v), &miner).unwrap_err();
    assert!(err.contains("EvmSignatureMalformed"), "{err}");
    // The zero address pads to the "never bound" bond key, so it can't be claimed.
    let err = send(&mut svm, bind_evm_ix(&m, [0u8; 20], good), &miner).unwrap_err();
    assert!(err.contains("EvmAddressZero"), "{err}");
    assert!(svm.get_account(&evm_bind_pda(&m)).is_none_or(|a| a.data.is_empty()), "nothing bound");
    send(&mut svm, bind_evm_ix(&m, addr, good), &miner).expect("the low-s original binds");
}

#[test]
fn test_evm_bind_message_matches_the_shared_vector() {
    // Pinned byte for byte in tests/test_solana_evm_bind.py: what eth_account signs, the program verifies.
    let addr = evm_addr(&EVM_KEY);
    let solana = Pubkey::new_from_array(core::array::from_fn(|i| i as u8 + 1));
    let msg = evm_bind_message(&addr, &solana, &pid());
    assert_eq!(
        msg,
        "allways: bind EVM address 0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266 to Solana \
         4wBqpZM9xaSheZzJSMawUKKwhdpChKbZ5eu5ky4Vigw (program 6JVBEj5w27J2SVjERmv2c7wXgFee9nSSBKUJevHehyBD)"
    );
    assert_eq!(hex(&evm_bind_digest(&msg)), "993a92a4d3f9a8d6c77e4f75460823c8b8e3cfa1650423c762129b9f186bf35e");
    assert_eq!(
        hex(&evm_sign(&EVM_KEY, &addr, &solana)),
        "c512197956bd508d3ba52b4a8788d1afff0f71da8db904503be60fc0d714adba\
         0357f8ae382560de32fbc704ac979be666c2cd93e1639875d9f98285d1b81eda1b"
    );
}

#[test]
fn test_evm_bind_fits_comfortably_in_the_default_compute_budget() {
    let (mut svm, _admin, _vals, miner) = setup();
    let m = miner.pubkey();
    let addr = evm_addr(&EVM_KEY);
    let ix = bind_evm_ix(&m, addr, evm_sign(&EVM_KEY, &addr, &m));
    let msg = Message::new_with_blockhash(&[ix], Some(&m), &svm.latest_blockhash());
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[&miner]).unwrap();
    let cu = svm.send_transaction(tx).expect("bind").compute_units_consumed;
    println!("bind_evm_address compute units: {cu}");
    assert!(cu < 100_000, "{cu}");
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// --- the pinned bond key, end to end on a Base-backed swap ---------------------------------------

#[test]
fn test_a_base_backed_swap_pins_the_evm_bond_key_and_the_verdict_carries_it() {
    let (mut svm, admin, vals, miner) = setup();
    let m = miner.pubkey();
    let v = &vals[0];
    // A hotkey binding too, to prove the Base swap pins the EVM key and not the hotkey.
    let bind_hk = Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::BindHotkey { hotkey: [9u8; 32], hotkey_sig: [1u8; 64] }.data(),
        allways_swap_manager::accounts::BindHotkey {
            miner: m,
            config: config_pda(),
            miner_state: Some(miner_pda(&m)),
            binding: bind_pda(&m),
            hotkey_binding: Pubkey::find_program_address(&[b"hkbind", &[9u8; 32]], &pid()).0,
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    );
    send(&mut svm, bind_hk, &miner).expect("bind hotkey");
    bind_evm(&mut svm, &miner, &EVM_KEY).expect("bind evm");

    open_base_policy(&mut svm, &admin);
    beat(&mut svm, &vals, BASE);
    attest(&mut svm, &vals, &m, BASE, USDC_MIN_COLLATERAL);
    quorum(&mut svm, &vals, |val| activate_ix(val, &m, BASE));
    assert_ne!(miner_state(&svm, &m).active_backings & BACKING_BIT_BASEUSDC, 0);
    send(&mut svm, quote_ix(&m, SOL, BASE, BASE), &miner).expect("base quote");

    draw(&mut svm, v, &m, SOL, BASE, BASE);
    // The baseusdc leg is the exact bind: collateral_amount must equal it.
    let err = send(&mut svm, finalize_ix(BASE, SOL, &v.pubkey(), &m, USDC_SWAP - 1, SOL_LEG, USDC_SWAP as u128), v)
        .unwrap_err();
    assert!(err.contains("InvalidAmount"), "{err}");
    send(&mut svm, finalize_ix(BASE, SOL, &v.pubkey(), &m, USDC_SWAP, SOL_LEG, USDC_SWAP as u128), v).expect("fill");
    send(&mut svm, claim_ix(BASE, &v.pubkey(), &m, "basetx"), v).expect("claim");
    let initiated_at = now_ts(&svm);
    quorum(&mut svm, &vals, |val| initiate_ix(BASE, SOL, val, &m, "basetx"));

    let key = swap_key("basetx");
    assert_eq!(swap_acct(&svm, &key).bond_key, padded(&evm_addr(&EVM_KEY)), "the EVM address, not the hotkey");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.reserved(BACKING_BIT_BASEUSDC), required_collateral(USDC_SWAP));
    assert_eq!(ms.reserved(BACKING_BIT_SOL), 0, "the SOL pot backs nothing here");

    set_clock(&mut svm, initiated_at + TIMEOUT_SECS + 1);
    let logs = quorum(&mut svm, &vals, |val| timeout_ix(val, &m, "basetx"));
    let ev: SwapTimedOut = event(&logs);
    assert_eq!(ev.bond_key, padded(&evm_addr(&EVM_KEY)));
    assert_eq!(ev.collateral_chain, BASE);
    assert_eq!(ev.slash, 0, "verdict only — the seizure is a vote on Base");
    assert_eq!(ev.penalty, required_collateral(USDC_SWAP));
    assert_eq!(ev.payee, "userDstAddr", "owed on the baseusdc leg");
    let ms = miner_state(&svm, &m);
    assert_eq!(ms.collateral, COLLATERAL, "no lamports moved");
    assert!(ms.settling_slot(BACKING_BIT_BASEUSDC) > 0, "busy-until-settled on the Base hub only");
}

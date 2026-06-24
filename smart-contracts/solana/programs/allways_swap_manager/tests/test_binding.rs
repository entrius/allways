// A5 — hotkey↔pubkey identity binding: bind_hotkey (LiteSVM, in-process).
//   cargo test -p allways_swap_manager --test test_binding
//
// bind_hotkey is miner-signed and per-miner (one Binding PDA). It only STORES the hotkey + sr25519
// sig (the validator verifies off-chain), reads no config, and overwrites in place on re-bind. No
// `initialize` is needed.
use {
    anchor_lang::{
        prelude::Pubkey, solana_program::instruction::Instruction, AccountDeserialize,
        InstructionData, ToAccountMetas,
    },
    allways_swap_manager::state::Binding,
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
};

const SYSTEM_PROGRAM: Pubkey = anchor_lang::solana_program::system_program::ID;

fn pid() -> Pubkey {
    allways_swap_manager::id()
}
fn bind_pda(miner: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"bind", miner.as_ref()], &pid()).0
}
fn hkbind_pda(hotkey: &[u8; 32]) -> Pubkey {
    Pubkey::find_program_address(&[b"hkbind", hotkey], &pid()).0
}
fn send(svm: &mut LiteSVM, ix: Instruction, payer: &Pubkey, signer: &Keypair) -> Result<(), String> {
    let blockhash = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[ix], Some(payer), &blockhash);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[signer]).unwrap();
    svm.send_transaction(tx).map(|_| ()).map_err(|e| format!("{:?}", e))
}
fn bind_ix(miner: &Pubkey, hotkey: [u8; 32], hotkey_sig: [u8; 64]) -> Instruction {
    Instruction::new_with_bytes(
        pid(),
        &allways_swap_manager::instruction::BindHotkey { hotkey, hotkey_sig }.data(),
        allways_swap_manager::accounts::BindHotkey {
            miner: *miner,
            binding: bind_pda(miner),
            hotkey_binding: hkbind_pda(&hotkey),
            system_program: SYSTEM_PROGRAM,
        }
        .to_account_metas(None),
    )
}
fn read_binding(svm: &LiteSVM, miner: &Pubkey) -> Binding {
    let a = svm.get_account(&bind_pda(miner)).unwrap();
    Binding::try_deserialize(&mut a.data.as_slice()).unwrap()
}
fn setup() -> (LiteSVM, Keypair) {
    let mut svm = LiteSVM::new();
    svm.add_program(pid(), include_bytes!("../../../target/deploy/allways_swap_manager.so")).unwrap();
    let miner = Keypair::new();
    svm.airdrop(&miner.pubkey(), 10_000_000_000).unwrap();
    (svm, miner)
}

#[test]
fn test_bind_creates_pda() {
    let (mut svm, miner) = setup();
    let hotkey = [9u8; 32];
    let sig = [3u8; 64];
    send(&mut svm, bind_ix(&miner.pubkey(), hotkey, sig), &miner.pubkey(), &miner).expect("bind");

    let b = read_binding(&svm, &miner.pubkey());
    assert_eq!(b.miner, miner.pubkey());
    assert_eq!(b.hotkey, hotkey);
    assert_eq!(b.hotkey_sig, sig);
    assert!(b.bound_at >= 0);
}

#[test]
fn test_rebind_overwrites_in_place() {
    let (mut svm, miner) = setup();
    send(&mut svm, bind_ix(&miner.pubkey(), [1u8; 32], [1u8; 64]), &miner.pubkey(), &miner).expect("bind1");
    // re-bind with a different hotkey/sig overwrites the same PDA
    let hotkey2 = [2u8; 32];
    let sig2 = [4u8; 64];
    send(&mut svm, bind_ix(&miner.pubkey(), hotkey2, sig2), &miner.pubkey(), &miner).expect("bind2");

    let b = read_binding(&svm, &miner.pubkey());
    assert_eq!(b.hotkey, hotkey2, "hotkey overwritten");
    assert_eq!(b.hotkey_sig, sig2, "sig overwritten");
    assert_eq!(b.miner, miner.pubkey(), "miner identity stable");
}

#[test]
fn test_hotkey_pinned_to_first_pubkey() {
    // Set-once reverse marker: a second, different pubkey can't claim a hotkey already bound — closes
    // the strike-dodge (rotate to a fresh pubkey + re-bind the same hotkey). The owner can still re-bind.
    let (mut svm, miner_a) = setup();
    let hotkey = [7u8; 32];
    send(&mut svm, bind_ix(&miner_a.pubkey(), hotkey, [1u8; 64]), &miner_a.pubkey(), &miner_a).expect("A binds");

    let miner_b = Keypair::new();
    svm.airdrop(&miner_b.pubkey(), 10_000_000_000).unwrap();
    let res = send(&mut svm, bind_ix(&miner_b.pubkey(), hotkey, [2u8; 64]), &miner_b.pubkey(), &miner_b);
    assert!(res.is_err(), "a different pubkey must not claim an already-bound hotkey");

    // The owner CAN still re-bind the same hotkey (refresh sig).
    send(&mut svm, bind_ix(&miner_a.pubkey(), hotkey, [9u8; 64]), &miner_a.pubkey(), &miner_a).expect("A re-binds");
    assert_eq!(read_binding(&svm, &miner_a.pubkey()).hotkey_sig, [9u8; 64], "owner refreshed its own sig");
}

"""SolUsdc — Circle USDC on Solana, the first SPL token beside native SOL. Offline: the RPC is a
method→response stub injected through the composed chain; keypairs and ATA derivation are real."""

import pytest
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from allways.assets import ASSET_REGISTRY
from allways.assets.asset import MissingTestnetDeployment, ProviderUnreachableError
from allways.assets.sol import RESERVED_ACCOUNTS, SolanaChain
from allways.assets.solusdc import SolUsdc
from allways.assets.spl_token import (
    ASSOCIATED_TOKEN_PROGRAM_ID,
    ATA_RENT_LAMPORTS,
    IX_CREATE_ATA_IDEMPOTENT,
    IX_TRANSFER_CHECKED,
    TOKEN_PROGRAM_ID,
    TX_FEE_LAMPORTS,
    associated_token_address,
)
from allways.chains import CHAIN_SOL, CHAIN_SOLUSDC, get_chain_def
from allways.constants import CANCEL_REASON_SOL_RESERVED, CANCEL_REASON_SPL_FROZEN, LAUNCH_SPOKES
from allways.solana.rpc import SOLANA_GENESIS_HASHES, SolanaRpcUnreachable

MAINNET_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
DEVNET_MINT = '4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU'
OTHER_MINT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'  # USDT — an impostor for a USDC leg
GENESIS = {v: k for k, v in SOLANA_GENESIS_HASHES.items()}
MINER = Keypair()
USER = Keypair()
FEE_PAYER = Keypair()
AMOUNT = 150_000_000  # 150 USDC


def ata(owner: Keypair | str, mint: str = MAINNET_MINT) -> str:
    owner = owner if isinstance(owner, str) else str(owner.pubkey())
    return str(associated_token_address(owner, Pubkey.from_string(mint)))


def token_tx(
    recipient=USER,
    credit=AMOUNT,
    sender=MINER,
    mint=MAINNET_MINT,
    slot=100,
    block_time=5000,
    err=None,
    fee_payer=None,
    drop_token_balances=False,
    sender_owner_field=True,
):
    """A getTransaction dict moving `credit` of `mint` from sender's ATA to recipient's ATA."""
    payer = str((fee_payer or sender).pubkey())
    src, dst = ata(sender, mint), ata(recipient, mint)
    keys = [payer, src, dst, mint, str(TOKEN_PROGRAM_ID)]

    def bal(idx, owner, amount):
        e = {'accountIndex': idx, 'mint': mint, 'uiTokenAmount': {'amount': str(amount), 'decimals': 6}}
        if sender_owner_field or idx != 1:
            e['owner'] = str(owner.pubkey())
        return e

    pre = [bal(1, sender, 1_000_000_000), bal(2, recipient, 10)]
    post = [bal(1, sender, 1_000_000_000 - credit), bal(2, recipient, 10 + credit)]
    meta = {'err': err, 'preBalances': [1] * 5, 'postBalances': [1] * 5}
    if not drop_token_balances:
        meta['preTokenBalances'], meta['postTokenBalances'] = pre, post
    return {'slot': slot, 'blockTime': block_time, 'meta': meta, 'transaction': {'message': {'accountKeys': keys}}}


def parsed_token_account(amount=AMOUNT * 10, state='initialized', owner=None):
    return {
        'owner': str(TOKEN_PROGRAM_ID),
        'lamports': ATA_RENT_LAMPORTS,
        'data': {'parsed': {'info': {'tokenAmount': {'amount': str(amount)}, 'state': state, 'owner': owner}}},
    }


def parsed_mint(decimals=6, owner=str(TOKEN_PROGRAM_ID)):
    return {'owner': owner, 'lamports': 1, 'data': {'parsed': {'info': {'decimals': decimals}}}}


class FakeRpc:
    def __init__(self, tx=None, slot=200, cluster='mainnet', accounts=None, lamports=10**9, raise_conn=False, txs=None):
        self._tx, self._slot, self._cluster, self._raise = tx, slot, cluster, raise_conn
        self._txs = txs  # sig -> tx, for the scanner
        self.accounts = accounts if accounts is not None else {}
        self._lamports = lamports
        self.sent = []
        self.sigs = []

    def get_genesis_hash(self):
        return GENESIS[self._cluster] if self._cluster in GENESIS else 'LoCaLnEt11111111111111111111111111111111111'

    def get_transaction(self, sig, commitment='confirmed'):
        if self._raise:
            raise SolanaRpcUnreachable('down', url='http://fake')
        if self._txs is not None:
            return self._txs.get(sig)
        return self._tx

    def get_slot(self, commitment='confirmed'):
        return self._slot

    def get_parsed_account(self, pubkey, commitment='confirmed'):
        if self._raise:
            raise SolanaRpcUnreachable('down', url='http://fake')
        return self.accounts.get(str(pubkey))

    def get_balance(self, pubkey, commitment='confirmed'):
        return self._lamports

    def get_signatures_for_address(self, addr, limit=20):
        return self.sigs

    def get_signature_statuses(self, sigs):
        return [None for _ in sigs]

    def get_latest_blockhash(self, commitment='confirmed'):
        return '11111111111111111111111111111111'

    def send_transaction(self, raw):
        self.sent.append(raw)
        return 'SIG' * 20

    def confirm(self, sig, timeout=30.0, poll=0.4):
        return {'slot': self._slot}


def provider_with(rpc, keypair=None, monkeypatch=None):
    if monkeypatch is not None:
        monkeypatch.delenv('SOLUSDC_TOKEN_MINT', raising=False)
    p = SolUsdc(solana_rpc_url='fake://rpc', solana_keypair=keypair)
    p.rpc = rpc
    return p


class TestRegistryRow:
    def test_bound_to_its_row_and_composes_solana(self, monkeypatch):
        p = provider_with(FakeRpc(), monkeypatch=monkeypatch)
        assert p.chain_def is CHAIN_SOLUSDC is get_chain_def('solusdc')
        assert 'solusdc' in LAUNCH_SPOKES
        assert isinstance(p.chain, SolanaChain) and p.chain is not p
        assert (CHAIN_SOLUSDC.decimals, CHAIN_SOLUSDC.native_unit) == (6, 'µUSDC')
        assert CHAIN_SOLUSDC.min_confirmations == CHAIN_SOL.min_confirmations
        assert CHAIN_SOLUSDC.host_chain == 'solana' and CHAIN_SOLUSDC.asset_locator == MAINNET_MINT

    def test_registry_hands_it_the_solana_rpc_and_keypair(self):
        spec = next(s for s in ASSET_REGISTRY if s.chain_id == 'solusdc')
        assert spec.cls is SolUsdc and spec.kwarg_names == ('solana_rpc_url', 'solana_keypair')


class TestMintResolution:
    def test_mainnet_uses_the_registry_mint(self, monkeypatch):
        p = provider_with(FakeRpc(cluster='mainnet'), monkeypatch=monkeypatch)
        assert str(p.mint) == MAINNET_MINT

    def test_devnet_uses_circles_devnet_mint(self, monkeypatch):
        p = provider_with(FakeRpc(cluster='devnet'), monkeypatch=monkeypatch)
        assert str(p.mint) == DEVNET_MINT

    def test_unknown_cluster_without_override_is_a_missing_deployment(self, monkeypatch):
        p = provider_with(FakeRpc(cluster='localnet'), monkeypatch=monkeypatch)
        with pytest.raises(MissingTestnetDeployment):
            p.mint

    def test_env_override_wins_without_touching_the_rpc(self, monkeypatch):
        monkeypatch.setenv('SOLUSDC_TOKEN_MINT', OTHER_MINT)
        p = SolUsdc(solana_rpc_url='fake://rpc')
        p.rpc = None  # any RPC use would blow up
        assert str(p.mint) == OTHER_MINT

    def test_construction_is_offline(self, monkeypatch):
        p = provider_with(FakeRpc(cluster='localnet'), monkeypatch=monkeypatch)  # no raise at construction
        assert p._mint is None


class TestAta:
    def test_matches_the_canonical_derivation(self):
        # Known vector: the ATA is a PDA of the associated-token program, never the owner itself.
        owner = str(USER.pubkey())
        derived = associated_token_address(owner, Pubkey.from_string(MAINNET_MINT))
        expect, _ = Pubkey.find_program_address(
            [bytes(USER.pubkey()), bytes(TOKEN_PROGRAM_ID), bytes(Pubkey.from_string(MAINNET_MINT))],
            ASSOCIATED_TOKEN_PROGRAM_ID,
        )
        assert derived == expect and str(derived) != owner


class TestCheckConnection:
    def test_happy_path(self, monkeypatch):
        accounts = {MAINNET_MINT: parsed_mint(), ata(MINER): parsed_token_account()}
        p = provider_with(FakeRpc(accounts=accounts), keypair=MINER, monkeypatch=monkeypatch)
        p.check_connection(require_send=True)

    def test_missing_keypair_fails_when_send_required(self, monkeypatch):
        p = provider_with(FakeRpc(accounts={MAINNET_MINT: parsed_mint()}), monkeypatch=monkeypatch)
        with pytest.raises(ConnectionError):
            p.check_connection(require_send=True)
        p.check_connection(require_send=False)

    def test_token_2022_mint_is_rejected(self, monkeypatch):
        accounts = {MAINNET_MINT: parsed_mint(owner='TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb')}
        p = provider_with(FakeRpc(accounts=accounts), monkeypatch=monkeypatch)
        with pytest.raises(ConnectionError, match='legacy SPL'):
            p.check_connection(require_send=False)

    def test_decimals_drift_is_rejected(self, monkeypatch):
        p = provider_with(FakeRpc(accounts={MAINNET_MINT: parsed_mint(decimals=9)}), monkeypatch=monkeypatch)
        with pytest.raises(ConnectionError, match='decimals'):
            p.check_connection(require_send=False)

    def test_absent_mint_is_rejected(self, monkeypatch):
        p = provider_with(FakeRpc(accounts={}), monkeypatch=monkeypatch)
        with pytest.raises(ConnectionError, match='does not exist'):
            p.check_connection(require_send=False)


class TestVerification:
    RECIP = str(USER.pubkey())
    SENDER = str(MINER.pubkey())

    def test_settled_transfer_matches_with_sender_pinned(self, monkeypatch):
        p = provider_with(FakeRpc(tx=token_tx(), slot=100 + 40), monkeypatch=monkeypatch)
        info = p.verify_transaction('sig', self.RECIP, AMOUNT, expected_sender=self.SENDER)
        assert info is not None and info.amount == AMOUNT and info.sender == self.SENDER
        assert info.confirmed and info.block_time == 5000 and info.recipient == self.RECIP

    def test_one_short_of_depth_is_unconfirmed(self, monkeypatch):
        p = provider_with(FakeRpc(tx=token_tx(), slot=100 + 30), monkeypatch=monkeypatch)
        info = p.fetch_matching_tx('sig', self.RECIP, AMOUNT)
        assert info is not None and not info.confirmed

    @pytest.mark.parametrize(
        'tx',
        [
            token_tx(err={'InstructionError': [0, 'Custom']}),
            token_tx(credit=AMOUNT - 1),
            token_tx(mint=OTHER_MINT),  # the right ATA shape for the wrong mint never credits USDC
            token_tx(recipient=FEE_PAYER),  # credited someone else's ATA
            None,
        ],
        ids=['failed-on-chain', 'underpaid', 'wrong-mint', 'wrong-recipient', 'not-found'],
    )
    def test_rejections_are_authoritative(self, tx, monkeypatch):
        p = provider_with(FakeRpc(tx=tx, slot=200), monkeypatch=monkeypatch)
        assert p.fetch_matching_tx('sig', self.RECIP, AMOUNT) is None

    def test_sender_is_the_token_owner_not_the_fee_payer(self, monkeypatch):
        p = provider_with(FakeRpc(tx=token_tx(fee_payer=FEE_PAYER), slot=200), monkeypatch=monkeypatch)
        info = p.fetch_matching_tx('sig', self.RECIP, AMOUNT)
        assert info.sender == self.SENDER
        assert p.verify_transaction('sig', self.RECIP, AMOUNT, expected_sender=str(FEE_PAYER.pubkey())) is None

    def test_unknowable_sender_fails_closed(self, monkeypatch):
        p = provider_with(FakeRpc(tx=token_tx(sender_owner_field=False), slot=200), monkeypatch=monkeypatch)
        assert p.fetch_matching_tx('sig', self.RECIP, AMOUNT).sender == ''
        assert p.verify_transaction('sig', self.RECIP, AMOUNT, expected_sender=self.SENDER) is None

    def test_self_transfer_is_rejected(self, monkeypatch):
        p = provider_with(FakeRpc(tx=token_tx(sender=USER), slot=200), monkeypatch=monkeypatch)
        assert p.verify_transaction('sig', self.RECIP, AMOUNT) is None

    def test_ata_pasted_as_recipient_never_matches(self, monkeypatch):
        # The address is an OWNER; an ATA handed in derives a different (nonexistent) ATA.
        p = provider_with(FakeRpc(tx=token_tx(), slot=200), monkeypatch=monkeypatch)
        assert p.fetch_matching_tx('sig', ata(USER), AMOUNT) is None

    def test_transport_failure_raises(self, monkeypatch):
        p = provider_with(FakeRpc(raise_conn=True), monkeypatch=monkeypatch)
        with pytest.raises(ProviderUnreachableError):
            p.fetch_matching_tx('sig', self.RECIP, AMOUNT)

    def test_missing_token_balances_is_unknown_not_absent(self, monkeypatch):
        p = provider_with(FakeRpc(tx=token_tx(drop_token_balances=True), slot=200), monkeypatch=monkeypatch)
        with pytest.raises(ProviderUnreachableError):
            p.fetch_matching_tx('sig', self.RECIP, AMOUNT)


class TestDeliveryGates:
    RECIP = str(USER.pubkey())

    def test_clean_destination_passes(self, monkeypatch):
        p = provider_with(FakeRpc(accounts={ata(USER): parsed_token_account()}), monkeypatch=monkeypatch)
        assert p.can_deliver_to(self.RECIP, AMOUNT) is True
        assert p.delivery_refused(self.RECIP, 0) is False
        assert p.cancel_evidence(self.RECIP, AMOUNT) is None

    def test_no_ata_yet_is_deliverable(self, monkeypatch):
        p = provider_with(FakeRpc(accounts={}), monkeypatch=monkeypatch)
        assert p.can_deliver_to(self.RECIP, AMOUNT) is True
        assert p.delivery_refused(self.RECIP, 0) is False

    def test_frozen_ata_bounces_reserve_and_is_cancel_evidence(self, monkeypatch):
        p = provider_with(FakeRpc(accounts={ata(USER): parsed_token_account(state='frozen')}), monkeypatch=monkeypatch)
        assert p.can_deliver_to(self.RECIP, AMOUNT) is False
        assert p.delivery_refused(self.RECIP, 0) is True
        assert p.cancel_evidence(self.RECIP, AMOUNT) == CANCEL_REASON_SPL_FROZEN

    def test_token_account_pasted_as_owner_bounces_reserve(self, monkeypatch):
        p = provider_with(FakeRpc(accounts={ata(USER): parsed_token_account()}), monkeypatch=monkeypatch)
        assert p.can_deliver_to(ata(USER), AMOUNT) is False

    def test_reserved_owner_is_refused_offline(self, monkeypatch):
        p = provider_with(FakeRpc(raise_conn=True), monkeypatch=monkeypatch)
        reserved = next(iter(RESERVED_ACCOUNTS))
        assert p.can_deliver_to(reserved, AMOUNT) is False
        assert p.delivery_refused(reserved, 0) is True
        assert p.cancel_evidence(reserved, AMOUNT) == CANCEL_REASON_SOL_RESERVED

    def test_reserve_gate_fails_open_but_slash_gate_raises(self, monkeypatch):
        p = provider_with(FakeRpc(raise_conn=True), monkeypatch=monkeypatch)
        assert p.can_deliver_to(self.RECIP, AMOUNT) is True
        with pytest.raises(Exception):
            p.delivery_refused(self.RECIP, 0)
        assert p.cancel_evidence(self.RECIP, AMOUNT) is None


class TestBalance:
    def test_reads_the_owners_ata(self, monkeypatch):
        p = provider_with(FakeRpc(accounts={ata(USER): parsed_token_account(amount=42)}), monkeypatch=monkeypatch)
        assert p.get_balance(str(USER.pubkey())) == 42

    def test_missing_ata_and_rpc_failure_read_zero(self, monkeypatch):
        assert provider_with(FakeRpc(accounts={}), monkeypatch=monkeypatch).get_balance(str(USER.pubkey())) == 0
        assert provider_with(FakeRpc(raise_conn=True), monkeypatch=monkeypatch).get_balance(str(USER.pubkey())) == 0


class TestSendGuards:
    TO = str(USER.pubkey())

    def rich(self, lamports=10**9, dst_ata=True):
        accounts = {ata(MINER): parsed_token_account(amount=AMOUNT * 10)}
        if dst_ata:
            accounts[ata(USER)] = parsed_token_account(amount=0)
        return FakeRpc(accounts=accounts, lamports=lamports)

    def test_no_key_refuses(self, monkeypatch):
        assert provider_with(self.rich(), monkeypatch=monkeypatch).send_amount(self.TO, AMOUNT) is None

    def test_key_mismatch_refuses(self, monkeypatch):
        p = provider_with(self.rich(), keypair=MINER, monkeypatch=monkeypatch)
        assert p.send_amount(self.TO, AMOUNT, from_address=str(FEE_PAYER.pubkey())) is None
        assert p.rpc.sent == []

    def test_token_poor_refuses_before_broadcasting(self, monkeypatch):
        rpc = self.rich()
        rpc.accounts[ata(MINER)] = parsed_token_account(amount=AMOUNT - 1)
        p = provider_with(rpc, keypair=MINER, monkeypatch=monkeypatch)
        assert p.send_amount(self.TO, AMOUNT) is None and rpc.sent == []

    def test_token_rich_sol_poor_refuses_before_broadcasting(self, monkeypatch):
        # Enough SOL for the fee but not for the rent of a destination ATA that doesn't exist yet.
        rpc = self.rich(lamports=TX_FEE_LAMPORTS + ATA_RENT_LAMPORTS - 1, dst_ata=False)
        p = provider_with(rpc, keypair=MINER, monkeypatch=monkeypatch)
        assert p.send_amount(self.TO, AMOUNT) is None and rpc.sent == []
        # Same SOL is plenty once the destination ATA exists (no rent to pay).
        rpc2 = self.rich(lamports=TX_FEE_LAMPORTS + ATA_RENT_LAMPORTS - 1, dst_ata=True)
        p2 = provider_with(rpc2, keypair=MINER, monkeypatch=monkeypatch)
        assert p2.send_amount(self.TO, AMOUNT) == ('SIG' * 20, 200)

    def test_happy_path_emits_create_ata_then_transfer_checked(self, monkeypatch):
        from solders.transaction import Transaction

        rpc = self.rich(dst_ata=False)
        p = provider_with(rpc, keypair=MINER, monkeypatch=monkeypatch)
        assert p.send_amount(self.TO, AMOUNT, from_address=str(MINER.pubkey()), dedup_key='swap-1') == ('SIG' * 20, 200)
        import base64

        tx = Transaction.from_bytes(base64.b64decode(rpc.sent[0]))
        ixs = tx.message.instructions
        keys = tx.message.account_keys
        assert len(ixs) == 2
        assert keys[ixs[0].program_id_index] == ASSOCIATED_TOKEN_PROGRAM_ID and bytes(ixs[0].data) == bytes(
            [IX_CREATE_ATA_IDEMPOTENT]
        )
        assert keys[ixs[1].program_id_index] == TOKEN_PROGRAM_ID
        assert bytes(ixs[1].data) == bytes([IX_TRANSFER_CHECKED]) + AMOUNT.to_bytes(8, 'little') + bytes([6])
        dst = keys[ixs[1].accounts[2]]
        assert str(dst) == ata(USER)
        # Recorded for the dedup guard under this obligation.
        assert list(p.chain.broadcasted_txids.values())[0][:3] == (self.TO, AMOUNT, 'swap-1')

    def test_prior_broadcast_is_reused_not_repaid(self, monkeypatch):
        rpc = self.rich()
        rpc.get_signature_statuses = lambda sigs: [{'confirmationStatus': 'finalized', 'err': None, 'slot': 7}]
        p = provider_with(rpc, keypair=MINER, monkeypatch=monkeypatch)
        p.chain.broadcasted_txids['OLD'] = (self.TO, AMOUNT, '', 150)
        assert p.send_amount(self.TO, AMOUNT) == ('OLD', 7)
        assert rpc.sent == []


class TestScanner:
    def test_finds_the_senders_payment_to_the_owners_ata(self, monkeypatch):
        rpc = FakeRpc(txs={'A': token_tx(sender=FEE_PAYER), 'B': token_tx()}, slot=200)
        rpc.sigs = [{'signature': 'A'}, {'signature': 'B'}]
        p = provider_with(rpc, monkeypatch=monkeypatch)
        assert p.find_recent_outgoing(str(MINER.pubkey()), str(USER.pubkey()), AMOUNT) == 'B'
        assert p.find_recent_outgoing(str(MINER.pubkey()), str(USER.pubkey()), AMOUNT + 1) is None

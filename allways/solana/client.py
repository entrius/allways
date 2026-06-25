"""AllwaysSolanaClient — read PDAs, build/send instructions, ingest events for allways_swap_manager.

Hand-rolled + sync. Replaces the ink!/Substrate `AllwaysContractClient`. B0 ships all account readers +
getProgramAccounts discovery + the tx build/sign/send pipeline + a representative write set (bind_hotkey,
set_quote, post/withdraw_collateral) + an event-log ingest skeleton. The remaining ~25 instruction
builders land in B1/B2 as the loop needs them.
"""

import base64
import time
from typing import List, Optional, Tuple

from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import Transaction

from allways.solana import layouts, pdas
from allways.solana.rpc import SolanaRpc

SYSTEM_PROGRAM = Pubkey.from_string('11111111111111111111111111111111')


class SolanaClientError(Exception):
    pass


class AllwaysSolanaClient:
    def __init__(
        self, rpc_url: str, program_id: Pubkey = pdas.PROGRAM_ID, keypair: Optional[Keypair] = None, timeout: int = 30
    ):
        self.rpc = SolanaRpc(rpc_url, timeout=timeout)
        self.program_id = program_id
        self.keypair = keypair

    # ---------- decode ----------
    def _decode(self, name: str, raw: bytes):
        disc = layouts.DISCRIMINATORS[name]
        if raw[:8] != disc:
            raise SolanaClientError(f'{name}: account discriminator mismatch')
        c = layouts.ACCOUNT_LAYOUTS[name].parse(raw[8:])
        for f in layouts.ACCOUNT_PUBKEY_FIELDS.get(name, []):
            c[f] = Pubkey.from_bytes(bytes(c[f]))
        return c

    def _get(self, name: str, pubkey: Pubkey):
        raw = self.rpc.get_account_info(pubkey)
        return None if raw is None else self._decode(name, raw)

    # ---------- readers ----------
    def get_config(self):
        return self._get('Config', pdas.config_pda(self.program_id))

    def get_treasury(self):
        return self._get('Treasury', pdas.treasury_pda(self.program_id))

    def get_miner_state(self, miner):
        return self._get('MinerState', pdas.miner_state_pda(miner, self.program_id))

    def get_binding(self, miner):
        return self._get('Binding', pdas.binding_pda(miner, self.program_id))

    def get_hotkey_binding(self, hotkey: bytes):
        return self._get('HotkeyBinding', pdas.hotkey_binding_pda(hotkey, self.program_id))

    def get_reservation(self, miner):
        return self._get('Reservation', pdas.reservation_pda(miner, self.program_id))

    def get_pool(self, miner):
        return self._get('Pool', pdas.pool_pda(miner, self.program_id))

    def get_swap(self, swap_key: bytes):
        return self._get('Swap', pdas.swap_pda(swap_key, self.program_id))

    def get_quote(self, miner, from_chain: str, to_chain: str):
        return self._get('MinerQuote', pdas.quote_pda(miner, from_chain, to_chain, self.program_id))

    def get_direction_stats(self, miner, from_chain: str, to_chain: str):
        return self._get('MinerDirectionStats', pdas.stats_pda(miner, from_chain, to_chain, self.program_id))

    def get_collateral_lamports(self, miner) -> Optional[int]:
        """Collateral balance = vault lamports (the SOL the miner posted; rent is included)."""
        return self.rpc.get_account_lamports(pdas.collateral_vault_pda(miner, self.program_id))

    # ---------- discovery (getProgramAccounts by discriminator) ----------
    def get_all(self, name: str) -> List[Tuple[str, object]]:
        out = []
        for pubkey, raw in self.rpc.get_program_accounts(self.program_id, disc8=layouts.DISCRIMINATORS[name]):
            out.append((pubkey, self._decode(name, raw)))
        return out

    def get_swaps(self, status: Optional[str] = None) -> List[Tuple[str, object]]:
        swaps = self.get_all('Swap')
        if status is None:
            return swaps
        return [(p, s) for p, s in swaps if type(s.status).__name__ == status]

    # ---------- tx pipeline ----------
    def _send(
        self,
        instructions: List[Instruction],
        signers: Optional[List[Keypair]] = None,
        skip_preflight: bool = False,
        retries: int = 5,
    ) -> str:
        if self.keypair is None:
            raise SolanaClientError('client has no keypair; cannot send transactions')
        signers = signers or [self.keypair]
        last_err: Optional[Exception] = None
        for _ in range(retries):
            blockhash = Hash.from_string(self.rpc.get_latest_blockhash())
            tx = Transaction.new_signed_with_payer(instructions, self.keypair.pubkey(), signers, blockhash)
            try:
                sig = self.rpc.send_transaction(base64.b64encode(bytes(tx)).decode(), skip_preflight=skip_preflight)
                self.rpc.confirm(sig)
                return sig
            except Exception as e:  # transient stale-blockhash on a fresh/lagging RPC → re-fetch + resend
                msg = str(e).lower()
                if 'blockhash' not in msg:
                    raise
                last_err = e
                time.sleep(0.5)
        raise SolanaClientError(f'send failed after {retries} blockhash retries: {last_err}')

    def _ix(self, name: str, arg_bytes: bytes, metas: List[AccountMeta]) -> Instruction:
        return Instruction(self.program_id, layouts.IX_DISCRIMINATORS[name] + arg_bytes, metas)

    # ---------- bootstrap (admin) ----------
    def initialize(
        self,
        min_collateral: int,
        max_collateral: int,
        fulfillment_timeout_secs: int,
        consensus_threshold_percent: int,
        min_swap_amount: int,
        max_swap_amount: int,
        reservation_ttl_secs: int,
    ) -> str:
        admin = self.keypair.pubkey()
        args = layouts.IX_INITIALIZE_ARGS.build(
            {
                'min_collateral': min_collateral,
                'max_collateral': max_collateral,
                'fulfillment_timeout_secs': fulfillment_timeout_secs,
                'consensus_threshold_percent': consensus_threshold_percent,
                'min_swap_amount': min_swap_amount,
                'max_swap_amount': max_swap_amount,
                'reservation_ttl_secs': reservation_ttl_secs,
            }
        )
        metas = [
            AccountMeta(admin, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, True),
            AccountMeta(pdas.treasury_pda(self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('initialize', args, metas)])

    # ---------- representative writes (miner-side, no consensus) ----------
    def bind_hotkey(self, hotkey: bytes, hotkey_sig: bytes) -> str:
        miner = self.keypair.pubkey()
        metas = [
            AccountMeta(miner, True, True),
            AccountMeta(pdas.binding_pda(miner, self.program_id), False, True),
            AccountMeta(pdas.hotkey_binding_pda(hotkey, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('bind_hotkey', bytes(hotkey) + bytes(hotkey_sig), metas)])

    def set_quote(
        self, from_chain: str, to_chain: str, miner_from_addr: str, miner_to_addr: str, rate: int, liquidity: int
    ) -> str:
        miner = self.keypair.pubkey()
        args = layouts.IX_SET_QUOTE_ARGS.build(
            {
                'from_chain': from_chain,
                'to_chain': to_chain,
                'miner_from_addr': miner_from_addr,
                'miner_to_addr': miner_to_addr,
                'rate': rate,
                'liquidity': liquidity,
            }
        )
        metas = [
            AccountMeta(miner, True, True),
            AccountMeta(pdas.quote_pda(miner, from_chain, to_chain, self.program_id), False, True),
            AccountMeta(pdas.treasury_pda(self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('set_quote', args, metas)])

    def post_collateral(self, amount: int) -> str:
        miner = self.keypair.pubkey()
        metas = [
            AccountMeta(miner, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(pdas.miner_state_pda(miner, self.program_id), False, True),
            AccountMeta(pdas.collateral_vault_pda(miner, self.program_id), False, True),
            AccountMeta(SYSTEM_PROGRAM, False, False),
        ]
        return self._send([self._ix('post_collateral', layouts.IX_AMOUNT_ARGS.build({'amount': amount}), metas)])

    def withdraw_collateral(self, amount: int) -> str:
        miner = self.keypair.pubkey()
        metas = [
            AccountMeta(miner, True, True),
            AccountMeta(pdas.config_pda(self.program_id), False, False),
            AccountMeta(pdas.miner_state_pda(miner, self.program_id), False, True),
            AccountMeta(pdas.collateral_vault_pda(miner, self.program_id), False, True),
        ]
        return self._send([self._ix('withdraw_collateral', layouts.IX_AMOUNT_ARGS.build({'amount': amount}), metas)])

    # ---------- event-log ingest skeleton (full decode-by-discriminator in B3) ----------
    def get_program_signatures(
        self, before: Optional[str] = None, until: Optional[str] = None, limit: int = 100
    ) -> List[dict]:
        return self.rpc.get_signatures_for_address(self.program_id, before=before, until=until, limit=limit)

    def get_event_logs(self, signature: str) -> List[bytes]:
        """Extract the base64 `Program data:` payloads from a tx's logs (Anchor self-CPI events)."""
        tx = self.rpc.get_transaction(signature)
        if not tx:
            return []
        logs = (tx.get('meta') or {}).get('logMessages') or []
        out = []
        for line in logs:
            if line.startswith('Program data: '):
                out.append(base64.b64decode(line[len('Program data: ') :]))
        return out

    # ---------- dev helper ----------
    def airdrop(self, pubkey, lamports: int) -> str:
        sig = self.rpc.request_airdrop(pubkey, lamports)
        self.rpc.confirm(sig)
        return sig

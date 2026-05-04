import os
import time
from typing import Any, Optional, Tuple

import base58
import bech32
import bittensor as bt
import requests
from bitcoin_message_tool.bmt import sign_message, verify_message

from allways.chain_providers.base import ChainProvider, ProviderUnreachableError, TransactionInfo
from allways.chains import CHAIN_BTC, ChainDefinition
from allways.constants import BTC_TO_SAT

ADDR_TYPE_P2PKH = 'p2pkh'
ADDR_TYPE_P2SH_P2WPKH = 'p2wpkh-p2sh'
ADDR_TYPE_P2WPKH = 'p2wpkh'
ADDR_TYPE_P2TR = 'p2tr'


def detect_address_type(address: str) -> str:
    """Detect Bitcoin address type from its prefix."""
    if address.startswith('bc1q'):
        return ADDR_TYPE_P2WPKH
    if address.startswith('bc1p'):
        return ADDR_TYPE_P2TR
    if address.startswith('3'):
        return ADDR_TYPE_P2SH_P2WPKH
    if address.startswith('1'):
        return ADDR_TYPE_P2PKH
    # Regtest/testnet prefixes
    if address.startswith('bcrt1q') or address.startswith('tb1q'):
        return ADDR_TYPE_P2WPKH
    if address.startswith('bcrt1p') or address.startswith('tb1p'):
        return ADDR_TYPE_P2TR
    if address.startswith('2'):
        return ADDR_TYPE_P2SH_P2WPKH
    if address.startswith('m') or address.startswith('n'):
        return ADDR_TYPE_P2PKH
    return 'unknown'


def to_mainnet_wif(wif: str) -> str:
    """Convert a testnet/regtest WIF (0xef) to mainnet (0x80) for signing libraries."""
    decoded = base58.b58decode_check(wif)
    if decoded[0] == 0xEF:
        return base58.b58encode_check(bytes([0x80]) + decoded[1:]).decode()
    return wif


def to_mainnet_address(address: str) -> str:
    """Convert a testnet/regtest address to mainnet equivalent for verification."""
    if address.startswith('bcrt1') or address.startswith('tb1'):
        hrp, data = bech32.bech32_decode(address)
        if data is not None:
            return bech32.bech32_encode('bc', data)
    if address.startswith(('m', 'n')):
        decoded = base58.b58decode_check(address)
        if decoded[0] == 0x6F:
            return base58.b58encode_check(bytes([0x00]) + decoded[1:]).decode()
    if address.startswith('2'):
        decoded = base58.b58decode_check(address)
        if decoded[0] == 0xC4:
            return base58.b58encode_check(bytes([0x05]) + decoded[1:]).decode()
    return address


class BitcoinProvider(ChainProvider):
    """Bitcoin chain provider. Supports two modes:

    - node: Uses a local Bitcoin Core JSON-RPC node (default)
    - lightweight: Uses embit + Blockstream API (no local node required)

    Set BTC_MODE=lightweight to run without a local node.
    """

    def __init__(self):
        self.mode = os.environ.get('BTC_MODE', 'node').lower()
        if self.mode not in ('node', 'lightweight'):
            raise ValueError(f"BTC_MODE must be 'node' or 'lightweight', got '{self.mode}'")

        self.network = os.environ.get('BTC_NETWORK', '').lower()

        if self.mode == 'node':
            self.rpc_url = os.environ.get('BTC_RPC_URL', 'http://localhost:8332')
            self.rpc_user = os.environ.get('BTC_RPC_USER', '')
            self.rpc_pass = os.environ.get('BTC_RPC_PASS', '')
            if not self.network:
                if any(p in self.rpc_url for p in [':18332', ':18443', 'testnet']):
                    self.network = 'testnet'
                else:
                    self.network = 'mainnet'
        else:
            self.rpc_url = ''
            self.rpc_user = ''
            self.rpc_pass = ''
            if not self.network:
                self.network = 'mainnet'

        # Disable HTTP keepalive: validators are long-running and the default
        # global session pools idle TLS sockets that Blockstream's CDN silently
        # drops, wedging subsequent reads until our timeout fires.
        self.http = requests.Session()
        self.http.headers['Connection'] = 'close'

        # Last failure reason from send_amount / send_amount_lightweight, so
        # callers can surface a useful message without scraping logs.
        self.last_send_error: Optional[str] = None

    def _send_error(self, msg: str) -> None:
        self.last_send_error = msg
        bt.logging.error(msg)

    def get_chain(self) -> ChainDefinition:
        return CHAIN_BTC

    def check_connection(self, require_send: bool = True) -> None:
        if self.mode == 'lightweight':
            if require_send and not os.environ.get('BTC_PRIVATE_KEY'):
                raise ConnectionError('BTC_MODE=lightweight requires BTC_PRIVATE_KEY env var')
            try:
                import embit  # noqa: F401
            except ImportError as e:
                raise ConnectionError('BTC_MODE=lightweight requires embit (pip install embit)') from e
            try:
                resp = self.btc_api_get('/blocks/tip/height', timeout=10)
                resp.raise_for_status()
                tip = int(resp.text.strip())
                bt.logging.success(f'BTC lightweight mode: network={self.network}, Esplora tip={tip}')
            except Exception as e:
                raise ConnectionError(f'Cannot reach Esplora API: {e}') from e
            return

        result = self.rpc_call('getblockchaininfo', [])
        if result is None:
            raise ConnectionError(f'Cannot reach Bitcoin RPC at {self.rpc_url}')
        bt.logging.success(f'BTC RPC connected: chain={result.get("chain")}, blocks={result.get("blocks")}')

    def rpc_call(self, method: str, params: Optional[list] = None) -> Optional[dict]:
        """Generic JSON-RPC helper for BTC Core."""
        if self.mode == 'lightweight':
            return None
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': method,
            'params': params or [],
        }
        try:
            auth = (self.rpc_user, self.rpc_pass) if self.rpc_user else None
            response = self.http.post(self.rpc_url, json=payload, auth=auth, timeout=30)
            response.raise_for_status()
            result = response.json()
            if result.get('error'):
                bt.logging.error(f'BTC RPC error ({method}): {result["error"]}')
                return None
            return result.get('result')
        except Exception as e:
            bt.logging.error(f'BTC RPC call failed ({method}): {e}')
            return None

    def fetch_matching_tx(
        self,
        tx_hash: str,
        expected_recipient: str,
        expected_amount: int,
        block_hint: int = 0,
        max_scan_blocks: int = 150,  # unused — BTC backends index by tx hash
    ) -> Optional[TransactionInfo]:
        """Look up a Bitcoin tx via RPC with Esplora fallback."""
        result = self.rpc_verify_transaction(tx_hash, expected_recipient, expected_amount)
        if result is not None:
            return result
        return self.api_verify_transaction(tx_hash, expected_recipient, expected_amount)

    def rpc_verify_transaction(
        self, tx_hash: str, expected_recipient: str, expected_amount: int
    ) -> Optional[TransactionInfo]:
        """Verify a Bitcoin transaction using getrawtransaction RPC."""
        raw_tx = self.rpc_call('getrawtransaction', [tx_hash, True])
        if not raw_tx:
            return None

        confirmations = raw_tx.get('confirmations', 0)
        confirmed = confirmations >= self.get_chain().min_confirmations
        block_number = None

        if confirmed and 'blockhash' in raw_tx:
            block_info = self.rpc_call('getblock', [raw_tx['blockhash']])
            if block_info:
                block_number = block_info.get('height')

        for vout in raw_tx.get('vout', []):
            addresses = vout.get('scriptPubKey', {}).get('addresses', [])
            if not addresses:
                addr = vout.get('scriptPubKey', {}).get('address')
                if addr:
                    addresses = [addr]

            amount_sat = int(round(vout.get('value', 0) * BTC_TO_SAT))

            if expected_recipient in addresses and amount_sat >= expected_amount:
                sender = self.rpc_resolve_sender(raw_tx)
                return TransactionInfo(
                    tx_hash=tx_hash,
                    confirmed=confirmed,
                    sender=sender,
                    recipient=expected_recipient,
                    amount=amount_sat,
                    block_number=block_number,
                    confirmations=confirmations,
                )

        return None

    def rpc_resolve_sender(self, raw_tx: dict) -> str:
        """Extract sender address from the first vin of a raw transaction."""
        if not raw_tx.get('vin'):
            return ''
        vin = raw_tx['vin'][0]
        if 'txid' not in vin:
            return ''
        prev_tx = self.rpc_call('getrawtransaction', [vin['txid'], True])
        if not prev_tx or not prev_tx.get('vout'):
            return ''
        vout_idx = vin.get('vout', 0)
        if vout_idx >= len(prev_tx['vout']):
            return ''
        prev_vout = prev_tx['vout'][vout_idx]
        prev_addrs = prev_vout.get('scriptPubKey', {}).get('addresses', [])
        if not prev_addrs:
            prev_addr = prev_vout.get('scriptPubKey', {}).get('address')
            if prev_addr:
                prev_addrs = [prev_addr]
        return prev_addrs[0] if prev_addrs else ''

    # --- Esplora API methods (blockstream.info + mempool.space fallback) ---
    # Used as primary data source in lightweight mode, and as fallback in node mode.

    def api_calc_confirmations(self, block_number: int) -> int:
        """Fetch the chain tip from Esplora and calculate confirmations for a block."""
        try:
            tip_resp = self.btc_api_get('/blocks/tip/height', timeout=10)
            if tip_resp.ok:
                tip_height = int(tip_resp.text.strip())
                return tip_height - block_number + 1
        except Exception:
            pass
        return 0

    def api_verify_transaction(
        self, tx_hash: str, expected_recipient: str, expected_amount: int
    ) -> Optional[TransactionInfo]:
        """Verify via Esplora API; raises ProviderUnreachableError if unreachable."""
        try:
            resp = self.btc_api_get(f'/tx/{tx_hash}', timeout=15)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()

            confirmed = data.get('status', {}).get('confirmed', False)
            block_number = data.get('status', {}).get('block_height')
            confirmations = 0

            if confirmed and block_number:
                confirmations = self.api_calc_confirmations(block_number)

            min_confs = self.get_chain().min_confirmations
            is_confirmed = confirmations >= min_confs if confirmed else False

            for vout in data.get('vout', []):
                addr = vout.get('scriptpubkey_address', '')
                amount_sat = vout.get('value', 0)

                if addr == expected_recipient and amount_sat >= expected_amount:
                    sender = ''
                    if data.get('vin'):
                        sender = data['vin'][0].get('prevout', {}).get('scriptpubkey_address', '')

                    return TransactionInfo(
                        tx_hash=tx_hash,
                        confirmed=is_confirmed,
                        sender=sender,
                        recipient=expected_recipient,
                        amount=amount_sat,
                        block_number=block_number,
                        confirmations=confirmations,
                    )

            return None
        except (requests.ConnectionError, requests.Timeout) as e:
            raise ProviderUnreachableError(f'Esplora API unreachable: {e}') from e
        except requests.HTTPError as e:
            raise ProviderUnreachableError(f'Esplora API error: {e}') from e
        except Exception as e:
            bt.logging.error(f'Esplora tx lookup failed for {tx_hash}: {e}')
            return None

    def get_balance(self, address: str) -> int:
        """Get balance for a Bitcoin address in satoshis via RPC with Esplora fallback."""
        result = self.rpc_call('getreceivedbyaddress', [address, 0])
        if result is not None:
            return int(round(result * BTC_TO_SAT))
        return self.api_get_balance(address)

    def btc_api_bases(self) -> Tuple[str, ...]:
        """Esplora-compatible bases tried in order; mempool.space is the fallback when blockstream is flaky."""
        if self.network == 'testnet':
            return (
                'https://blockstream.info/testnet/api',
                'https://mempool.space/testnet/api',
            )
        return (
            'https://blockstream.info/api',
            'https://mempool.space/api',
        )

    def btc_api_get(self, path: str, timeout: int = 15) -> requests.Response:
        last_err: Optional[Exception] = None
        for base in self.btc_api_bases():
            try:
                resp = self.http.get(f'{base}{path}', timeout=timeout)
                if resp.status_code >= 500:
                    last_err = requests.HTTPError(f'{base}{path}: {resp.status_code}', response=resp)
                    continue
                return resp
            except Exception as e:
                last_err = e
        raise last_err or RuntimeError('all BTC APIs failed')

    def btc_api_post(self, path: str, data, timeout: int = 30) -> requests.Response:
        last_err: Optional[Exception] = None
        for base in self.btc_api_bases():
            try:
                resp = self.http.post(f'{base}{path}', data=data, timeout=timeout)
                if resp.status_code >= 500:
                    last_err = requests.HTTPError(f'{base}{path}: {resp.status_code}', response=resp)
                    continue
                return resp
            except Exception as e:
                last_err = e
        raise last_err or RuntimeError('all BTC APIs failed')

    def tx_exists(self, txid: str) -> bool:
        try:
            return self.btc_api_get(f'/tx/{txid}', timeout=10).status_code == 200
        except Exception:
            return False

    def find_recent_outgoing(self, from_addr: str, to_addr: str, amount: int) -> Optional[str]:
        """Return tx hash if a recent (mempool or last 2 min) tx from from_addr pays exactly amount sat to to_addr.

        The 2-minute window is sized to catch a same-session retry after a broadcast timeout while
        excluding human-paced repeat sends to the same miner for the same amount.
        """
        try:
            resp = self.btc_api_get(f'/address/{from_addr}/txs', timeout=10)
            resp.raise_for_status()
        except Exception:
            return None
        cutoff = int(time.time()) - 120
        for tx in resp.json() or []:
            status = tx.get('status') or {}
            if status.get('confirmed') and status.get('block_time', 0) < cutoff:
                continue
            if not any(
                (vin.get('prevout') or {}).get('scriptpubkey_address') == from_addr for vin in tx.get('vin', [])
            ):
                continue
            for vout in tx.get('vout', []):
                if vout.get('scriptpubkey_address') == to_addr and int(vout.get('value', 0)) == amount:
                    return tx.get('txid')
        return None

    def api_get_balance(self, address: str) -> int:
        """Get balance via Esplora API. Returns satoshis."""
        try:
            resp = self.btc_api_get(f'/address/{address}', timeout=15)
            resp.raise_for_status()
            data = resp.json()
            funded = data.get('chain_stats', {}).get('funded_txo_sum', 0)
            spent = data.get('chain_stats', {}).get('spent_txo_sum', 0)
            mempool_funded = data.get('mempool_stats', {}).get('funded_txo_sum', 0)
            mempool_spent = data.get('mempool_stats', {}).get('spent_txo_sum', 0)
            return (funded - spent) + (mempool_funded - mempool_spent)
        except Exception as e:
            bt.logging.error(f'Esplora balance lookup failed for {address}: {e}')
            return 0

    def is_valid_address(self, address: str) -> bool:
        """Validate a Bitcoin address locally (bech32/base58 decode)."""
        return self.validate_address_local(address)

    def validate_address_local(self, address: str) -> bool:
        """Validate BTC address format without RPC (bech32/base58 decode)."""
        if not address or not isinstance(address, str):
            return False
        try:
            if address.lower().startswith(('bc1', 'tb1', 'bcrt1')):
                hrp, data = bech32.bech32_decode(address)
                return data is not None
            decoded = base58.b58decode_check(address)
            return len(decoded) == 21 and decoded[0] in (0x00, 0x05, 0x6F, 0xC4)
        except Exception:
            return False

    def get_wif(self, address: str) -> Optional[str]:
        """Get WIF private key from env var or Bitcoin Core wallet."""
        wif = os.environ.get('BTC_PRIVATE_KEY')
        if wif:
            if wif[0] not in '5KLc9':
                bt.logging.error(
                    f'BTC_PRIVATE_KEY is not a valid WIF (prefix {wif[:4]!r}); '
                    'expected 5/K/L (mainnet) or 9/c (test/regtest)'
                )
                return None
            return wif
        if self.mode == 'lightweight':
            bt.logging.error('BTC_MODE=lightweight requires BTC_PRIVATE_KEY env var for key operations')
            return None
        result = self.rpc_call('dumpprivkey', [address])
        return result if isinstance(result, str) else None

    def sign_from_proof(self, address: str, message: str, key: Optional[Any] = None) -> str:
        """Sign a message proving ownership of a Bitcoin address.

        Supports P2PKH, P2WPKH, and P2SH-P2WPKH addresses via BIP-137.
        key: WIF private key string. If None, attempts dumpprivkey RPC.
        """
        addr_type = detect_address_type(address)
        if addr_type == ADDR_TYPE_P2TR:
            bt.logging.error('Taproot (P2TR) addresses are not yet supported for message signing')
            return ''
        if addr_type == 'unknown':
            bt.logging.error(f'Unknown Bitcoin address type: {address}')
            return ''

        wif = key if isinstance(key, str) else self.get_wif(address)
        if not wif:
            bt.logging.error(
                'No WIF private key available for signing (set BTC_PRIVATE_KEY or ensure address is in wallet)'
            )
            return ''

        try:
            _, _, signature = sign_message(to_mainnet_wif(wif), addr_type, message, deterministic=True)
            return signature
        except Exception as e:
            bt.logging.error(f'BTC sign_from_proof failed: {e}')
            return ''

    def verify_from_proof(self, address: str, message: str, signature: str) -> bool:
        """Verify a signed message from a Bitcoin address.

        Supports P2PKH, P2WPKH, and P2SH-P2WPKH addresses via BIP-137.
        No RPC dependency — pure cryptographic verification.
        """
        addr_type = detect_address_type(address)
        if addr_type == ADDR_TYPE_P2TR:
            bt.logging.warning('Taproot (P2TR) addresses are not yet supported for message verification')
            return False
        if addr_type == 'unknown':
            bt.logging.error(f'Unknown Bitcoin address type for verification: {address}')
            return False

        try:
            valid, _, _ = verify_message(to_mainnet_address(address), message, signature)
            return valid
        except Exception as e:
            bt.logging.error(f'BTC verify_from_proof failed: {e}')
            return False

    def send_amount_lightweight(
        self,
        to_address: str,
        amount: int,
        from_address: Optional[str] = None,
        fee_rate_override: Optional[int] = None,
    ) -> Optional[Tuple[str, int]]:
        """Send BTC via embit + Blockstream API (no full node required). Amount in satoshis.

        Uses BTC_PRIVATE_KEY env var (WIF format). Supports all address types:
        P2WPKH (bc1q...), P2SH-P2WPKH (3...), and P2PKH (1...).

        If from_address is provided (e.g. the miner's committed address), the
        matching address type is derived directly from the WIF key. Otherwise,
        all types are probed to find where UTXOs exist.

        ``fee_rate_override`` (sat/vB) skips estimation and the network floor.

        Does NOT work on regtest (no public APIs). Returns (tx_hash, 0) or None.
        """
        self.last_send_error = None
        try:
            from embit.ec import PrivateKey as EmbitPrivateKey
            from embit.networks import NETWORKS
            from embit.psbt import PSBT
            from embit.script import Witness, address_to_scriptpubkey, p2pkh, p2sh, p2wpkh
            from embit.transaction import Transaction, TransactionInput, TransactionOutput
        except ImportError:
            self._send_error('embit not installed (pip install embit)')
            return None

        wif = os.environ.get('BTC_PRIVATE_KEY')
        if not wif:
            self._send_error('BTC_PRIVATE_KEY not set')
            return None

        try:
            network = NETWORKS['test'] if self.network == 'testnet' else NETWORKS['main']
            privkey = EmbitPrivateKey.from_wif(wif)
            pubkey = privkey.get_public_key()
            segwit_script = p2wpkh(pubkey)

            type_to_script = {
                ADDR_TYPE_P2WPKH: ('p2wpkh', segwit_script, segwit_script.address(network)),
                ADDR_TYPE_P2SH_P2WPKH: ('p2sh-p2wpkh', p2sh(segwit_script), p2sh(segwit_script).address(network)),
                ADDR_TYPE_P2PKH: ('p2pkh', p2pkh(pubkey), p2pkh(pubkey).address(network)),
            }

            result = self.resolve_sender_utxos(from_address, type_to_script)
            if result is None:
                return None
            my_script, my_address, utxos, addr_type = result

            existing = self.find_recent_outgoing(my_address, to_address, amount)
            if existing:
                bt.logging.info(f'Reusing prior tx {existing} from {my_address} → {to_address} ({amount} sat)')
                return (existing, 0)

            is_segwit = addr_type in ('p2wpkh', 'p2sh-p2wpkh')
            bt.logging.info(f'Sending from {addr_type} address: {my_address}')

            coin_selection = self.select_utxos(utxos, amount, is_segwit, fee_rate_override=fee_rate_override)
            if coin_selection is None:
                return None
            selected, total_in, fee = coin_selection
            change = total_in - amount - fee

            # Build transaction
            tx = Transaction(version=2, locktime=0)
            for utxo in selected:
                txid_bytes = bytes.fromhex(utxo['txid'])
                tx.vin.append(TransactionInput(txid_bytes, utxo['vout']))

            to_script = address_to_scriptpubkey(to_address)
            if to_script is None:
                self._send_error(f'Could not derive scriptPubKey for destination {to_address}')
                return None
            tx.vout.append(TransactionOutput(amount, to_script))
            if change > 546:  # dust threshold
                tx.vout.append(TransactionOutput(change, my_script))

            # Sign transaction
            psbt = PSBT(tx)
            if is_segwit:
                for i, utxo in enumerate(selected):
                    psbt.inputs[i].witness_utxo = TransactionOutput(utxo['value'], my_script)
                    if addr_type == 'p2sh-p2wpkh':
                        # Nested segwit: need redeem script
                        psbt.inputs[i].redeem_script = segwit_script
            else:
                # Legacy P2PKH: need full previous transaction for signing
                for i, utxo in enumerate(selected):
                    tx_resp = self.btc_api_get(f'/tx/{utxo["txid"]}/hex', timeout=20)
                    tx_resp.raise_for_status()
                    prev_tx = Transaction.from_string(tx_resp.text.strip())
                    psbt.inputs[i].non_witness_utxo = prev_tx

            for i, inp in enumerate(psbt.inputs):
                if inp.witness_utxo is None and inp.non_witness_utxo is None:
                    sel = selected[i]
                    self._send_error(f'PSBT input {i} missing utxo (txid={sel.get("txid")}, vout={sel.get("vout")})')
                    return None

            num_sigs = psbt.sign_with(privkey)
            if num_sigs != len(selected):
                self._send_error(f'Expected {len(selected)} sigs, got {num_sigs}')
                return None

            # Finalize: extract tx (psbt.tx returns a copy each time) and attach signatures
            final_tx = psbt.tx
            for i, inp in enumerate(psbt.inputs):
                if is_segwit:
                    for pub, sig in inp.partial_sigs.items():
                        final_tx.vin[i].witness = Witness([sig, pub.sec()])
                        if addr_type == 'p2sh-p2wpkh':
                            final_tx.vin[i].script_sig = segwit_script.serialize()
                else:
                    from embit.script import Script

                    for pub, sig in inp.partial_sigs.items():
                        sig_bytes = sig if isinstance(sig, bytes) else bytes(sig)
                        pub_bytes = pub.sec()
                        final_tx.vin[i].script_sig = Script(
                            bytes([len(sig_bytes)]) + sig_bytes + bytes([len(pub_bytes)]) + pub_bytes
                        )

            raw_tx = final_tx.serialize().hex()
            tx_hash = self.broadcast_tx(raw_tx)
            if tx_hash is None:
                return None

            bt.logging.info(f'Sent {amount} sat to {to_address} via embit (tx: {tx_hash}, fee: {fee})')
            return (tx_hash, 0)
        except Exception as e:
            import traceback

            bt.logging.debug(f'embit send traceback:\n{traceback.format_exc()}')
            self._send_error(f'embit send failed: {type(e).__name__}: {e}')
            return None

    def resolve_sender_utxos(self, from_address, type_to_script):
        """Match from_address to address type and fetch UTXOs, or probe all types."""
        if from_address:
            detected = detect_address_type(from_address)
            if detected not in type_to_script:
                self._send_error(f'Unsupported address type for {from_address}: {detected}')
                return None
            atype, script, addr = type_to_script[detected]
            if addr != from_address:
                self._send_error(f'WIF key derives {addr} but committed address is {from_address} — key mismatch')
                return None
            resp = self.btc_api_get(f'/address/{addr}/utxo', timeout=15)
            resp.raise_for_status()
            utxos = resp.json()
            if not utxos:
                self._send_error(f'No UTXOs found for {from_address}')
                return None
            return script, addr, utxos, atype

        import time as _time

        for idx, (atype, script, addr) in enumerate(type_to_script.values()):
            try:
                if idx > 0:
                    _time.sleep(1)
                resp = self.btc_api_get(f'/address/{addr}/utxo', timeout=15)
                resp.raise_for_status()
                candidate_utxos = resp.json()
                if candidate_utxos:
                    bt.logging.debug(f'Found UTXOs on {atype} address: {addr}')
                    return script, addr, candidate_utxos, atype
            except Exception:
                continue

        self._send_error('No UTXOs found for any address type')
        return None

    def select_utxos(self, utxos, amount: int, is_segwit: bool, fee_rate_override: Optional[int] = None):
        """Greedy UTXO selection. Returns (selected, total_in, fee) or None."""
        fee_rate = self.estimate_fee_rate(override=fee_rate_override)
        input_vsize = 68 if is_segwit else 148
        selected = []
        total_in = 0
        for utxo in sorted(utxos, key=lambda u: u['value'], reverse=True):
            selected.append(utxo)
            total_in += utxo['value']
            est_vsize = 11 + len(selected) * input_vsize + 2 * 31
            fee = est_vsize * fee_rate
            if total_in >= amount + fee:
                break

        est_vsize = 11 + len(selected) * input_vsize + 2 * 31
        fee = est_vsize * fee_rate
        if total_in < amount + fee:
            self._send_error(f'Insufficient funds: have {total_in} sat, need {amount} + {fee} fee')
            return None
        return selected, total_in, fee

    def broadcast_tx(self, raw_hex: str) -> Optional[str]:
        """Broadcast a raw transaction. Returns tx_hash or None."""
        expected_txid: Optional[str] = None
        try:
            from embit.transaction import Transaction as EmbitTx

            expected_txid = EmbitTx.from_string(raw_hex).txid().hex()
        except Exception:
            pass

        try:
            resp = self.btc_api_post('/tx', data=raw_hex, timeout=30)
        except Exception as e:
            if expected_txid and self.tx_exists(expected_txid):
                return expected_txid
            self._send_error(f'Broadcast failed: {e}')
            return None

        if resp.status_code != 200:
            if expected_txid and self.tx_exists(expected_txid):
                return expected_txid
            self._send_error(f'Broadcast rejected ({resp.status_code}): {resp.text.strip()}')
            return None
        return resp.text.strip()

    def estimate_fee_rate(self, override: Optional[int] = None) -> int:
        """Estimate fee rate (sat/vbyte) from Blockstream.

        Targets 2-3 block confirmation (~20-30 min) with a small safety pad on
        top, so a swap source tx reliably clears within the reservation
        without overpaying for next-block urgency. ``override`` (sat/vB) skips
        estimation, floor, and multiplier — caller is taking explicit
        responsibility for the rate (e.g. CPFP / manual bump).

        One retry with a 3s sleep on API failure — Blockstream's testnet
        endpoint occasionally returns 5xx; silent fallback to the floor has
        stranded at least one dest tx at 5 sat/vB.
        """
        if override is not None:
            return max(1, override)

        from allways.constants import BTC_FEE_RATE_SAFETY_MULTIPLIER, BTC_MIN_FEE_RATE

        for attempt in range(2):
            try:
                resp = self.btc_api_get('/fee-estimates', timeout=10)
                resp.raise_for_status()
                estimates = resp.json()
                for target in ('2', '3'):
                    if target in estimates:
                        padded = int(round(float(estimates[target]) * BTC_FEE_RATE_SAFETY_MULTIPLIER))
                        return max(BTC_MIN_FEE_RATE, padded)
            except Exception as e:
                bt.logging.debug(f'estimate_fee_rate: attempt {attempt + 1} failed: {e}')
            if attempt == 0:
                time.sleep(3)
        return BTC_MIN_FEE_RATE

    def send_amount(
        self, to_address: str, amount: int, from_address: Optional[str] = None
    ) -> Optional[Tuple[str, int]]:
        """Send BTC. Lightweight: embit + Blockstream. Node: RPC. Returns (tx_hash, block_number) or None.

        When ``from_address`` is given in node mode, inputs are pinned to UTXOs
        at that address. Plain ``sendtoaddress`` lets Core pick UTXOs from any
        address in the wallet — including auto-generated change addresses left
        over from prior sends — so the tx's first-input sender drifts off the
        miner's committed address and validators reject the fulfillment.

        Signing credentials come from ``BTC_PRIVATE_KEY`` / ``bitcoind`` wallet,
        not from the caller.
        """
        if self.mode == 'lightweight':
            return self.send_amount_lightweight(to_address, amount, from_address=from_address)

        self.last_send_error = None
        btc_amount = amount / BTC_TO_SAT
        if from_address:
            tx_hash = self.rpc_send_from_address(from_address, to_address, btc_amount)
        else:
            tx_hash = self.rpc_call('sendtoaddress', [to_address, btc_amount])
        if not tx_hash or not isinstance(tx_hash, str):
            self._send_error(f'BTC send failed for {amount} sat to {to_address}')
            return None

        block_count = self.rpc_call('getblockcount', [])
        block_number = (block_count + 1) if isinstance(block_count, int) else 0
        bt.logging.info(f'Sent {amount} sat ({btc_amount} BTC) to {to_address} (tx: {tx_hash})')
        return (tx_hash, block_number)

    def rpc_send_from_address(self, from_address: str, to_address: str, btc_amount: float) -> Optional[str]:
        """Send ``btc_amount`` to ``to_address`` using only UTXOs owned by ``from_address``.

        Required so the resulting tx's first-input sender equals the miner's
        committed address — validators enforce this and Core's default UTXO
        selection does not (it freely spends change addresses from prior sends).

        Flow: listunspent → createrawtransaction → fundrawtransaction with
        ``add_inputs=False`` so Core can't top up from other addresses, with
        ``changeAddress=from_address`` so change returns to the committed
        address rather than a fresh one.
        """
        utxos = self.rpc_call('listunspent', [1, 9999999, [from_address]]) or []
        if not utxos:
            bt.logging.error(f'BTC send: no spendable UTXOs at {from_address}')
            return None
        inputs = [{'txid': u['txid'], 'vout': u['vout']} for u in utxos]
        raw = self.rpc_call('createrawtransaction', [inputs, {to_address: btc_amount}])
        if not raw or not isinstance(raw, str):
            bt.logging.error(f'BTC createrawtransaction failed for {from_address} -> {to_address}')
            return None
        funded = self.rpc_call(
            'fundrawtransaction',
            [raw, {'changeAddress': from_address, 'add_inputs': False}],
        )
        if not funded or not funded.get('hex'):
            bt.logging.error(f'BTC fundrawtransaction failed for {from_address} -> {to_address}')
            return None
        signed = self.rpc_call('signrawtransactionwithwallet', [funded['hex']])
        if not signed or not signed.get('complete'):
            bt.logging.error(f'BTC signrawtransactionwithwallet incomplete: {signed}')
            return None
        return self.rpc_call('sendrawtransaction', [signed['hex']])

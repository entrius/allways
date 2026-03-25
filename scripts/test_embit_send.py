"""Quick test for embit send — verifies address detection and tx building without broadcasting.

Usage:
    BTC_PRIVATE_KEY=<your_wif> python scripts/test_embit_send.py [to_address] [amount_sat]
    BTC_PRIVATE_KEY=<your_wif> python scripts/test_embit_send.py <to_address> <amount_sat> --broadcast
"""

import os
import sys

import requests
from embit.ec import PrivateKey as EmbitPrivateKey
from embit.networks import NETWORKS
from embit.psbt import PSBT
from embit.script import Witness, address_to_scriptpubkey, p2pkh, p2sh, p2wpkh
from embit.transaction import Transaction, TransactionInput, TransactionOutput

BLOCKSTREAM_API = 'https://blockstream.info/api'


def main():
    wif = os.environ.get('BTC_PRIVATE_KEY')
    if not wif:
        print('Set BTC_PRIVATE_KEY env var')
        return

    to_address = sys.argv[1] if len(sys.argv) > 1 else 'bc1qcx9m46fajtdtplxchm9trt9suxjnkl0ykpt7e5'
    amount = int(sys.argv[2]) if len(sys.argv) > 2 else 50000

    network = NETWORKS['main']
    privkey = EmbitPrivateKey.from_wif(wif)
    pubkey = privkey.get_public_key()

    segwit_script = p2wpkh(pubkey)
    candidates = [
        ('p2wpkh', segwit_script, segwit_script.address(network)),
        ('p2sh-p2wpkh', p2sh(segwit_script), p2sh(segwit_script).address(network)),
        ('p2pkh', p2pkh(pubkey), p2pkh(pubkey).address(network)),
    ]

    import time

    print('Checking address types:')
    my_script = None
    my_address = None
    utxos = None
    addr_type = None
    for atype, script, addr in candidates:
        try:
            resp = requests.get(f'{BLOCKSTREAM_API}/address/{addr}/utxo', timeout=15)
            candidate_utxos = resp.json()
            balance = sum(u['value'] for u in candidate_utxos)
            print(f'  {atype:15s} {addr}  UTXOs={len(candidate_utxos)}  balance={balance} sat')
            if candidate_utxos and my_script is None:
                my_script = script
                my_address = addr
                utxos = candidate_utxos
                addr_type = atype
            time.sleep(5)
        except Exception as e:
            print(f'  {atype:15s} {addr}  ERROR: {e}')

    if not utxos:
        print('\nNo UTXOs found on any address type!')
        return

    print(f'\nUsing {addr_type} address: {my_address}')
    print(f'Sending {amount} sat to {to_address}')

    is_segwit = addr_type in ('p2wpkh', 'p2sh-p2wpkh')
    input_vsize = 68 if is_segwit else 148

    # Select UTXOs
    selected = []
    total_in = 0
    fee_rate = 5  # sat/vbyte conservative
    for utxo in sorted(utxos, key=lambda u: u['value'], reverse=True):
        selected.append(utxo)
        total_in += utxo['value']
        est_vsize = 11 + len(selected) * input_vsize + 2 * 31
        fee = est_vsize * fee_rate
        if total_in >= amount + fee:
            break

    est_vsize = 11 + len(selected) * input_vsize + 2 * 31
    fee = est_vsize * fee_rate
    change = total_in - amount - fee

    print(f'  Inputs: {len(selected)}, total_in={total_in}, fee={fee}, change={change}')

    if total_in < amount + fee:
        print('Insufficient funds!')
        return

    # Build tx
    tx = Transaction(version=2, locktime=0)
    for utxo in selected:
        txid_bytes = bytes.fromhex(utxo['txid'])
        tx.vin.append(TransactionInput(txid_bytes, utxo['vout']))

    tx.vout.append(TransactionOutput(amount, address_to_scriptpubkey(to_address)))
    if change > 546:
        tx.vout.append(TransactionOutput(change, my_script))

    # Sign
    psbt = PSBT(tx)
    if is_segwit:
        for i, utxo in enumerate(selected):
            psbt.inputs[i].witness_utxo = TransactionOutput(utxo['value'], my_script)
            if addr_type == 'p2sh-p2wpkh':
                psbt.inputs[i].redeem_script = segwit_script
    else:
        for i, utxo in enumerate(selected):
            tx_url = f'{BLOCKSTREAM_API}/tx/{utxo["txid"]}/hex'
            tx_resp = requests.get(tx_url, timeout=15)
            prev_tx = Transaction.from_string(tx_resp.text.strip())
            psbt.inputs[i].non_witness_utxo = prev_tx

    num_sigs = psbt.sign_with(privkey)
    print(f'  Signatures: {num_sigs}/{len(selected)}')

    if num_sigs != len(selected):
        print('SIGNING FAILED — not all inputs signed')
        return

    # Finalize (psbt.tx returns a copy each time, so extract once)
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
    print(f'\n  Raw TX ({len(raw_tx) // 2} bytes): {raw_tx[:80]}...')
    print('\n  To actually broadcast, run:')
    print(f'    curl -X POST -d "{raw_tx}" {BLOCKSTREAM_API}/tx')
    print('\n  Or pass --broadcast flag to send it')

    if '--broadcast' in sys.argv:
        print('\n  Broadcasting in 5s...')
        time.sleep(5)
        resp = requests.post(f'{BLOCKSTREAM_API}/tx', data=raw_tx, timeout=15)
        if resp.status_code == 200:
            print(f'\n  SENT! tx_hash: {resp.text.strip()}')
        else:
            print(f'\n  REJECTED ({resp.status_code}): {resp.text.strip()}')


if __name__ == '__main__':
    main()

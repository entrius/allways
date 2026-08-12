#!/usr/bin/env python3
"""Deploy the allways bond vault by UNLOCKING a btcli wallet with its password.

Modeled on gt-utils/dev-environment/scripts/deploy_contract.py: uses substrate-interface's
ContractCode + the bittensor wallet SDK, so you enter your WALLET PASSWORD (never a raw seed) and
the deployer keypair signs the instantiate extrinsic directly — no cargo-contract, no --suri.

`Wallet(name).coldkey` is a NON-destructive unlock (it does not rewrite the keyfile plaintext, unlike
keyfile.decrypt()). The vault deploys IMMUTABLE, so the constructor args below ARE the deployment.

Run (substrate-interface is pulled in ephemerally by uv):
  cd <repo root>
  uv run --with substrate-interface python smart-contracts/ink-bond-vault/deploy_vault.py \
    --wallet-name asm \
    --url wss://test.finney.opentensor.ai:443 --netuid 19 \
    --staking-hotkey 5HicmHG7fjbxrtx8FZNdv4xxS5jSN84KGpMnTHsKtKv9peao \
    --validators 5HicmHG7fjbxrtx8FZNdv4xxS5jSN84KGpMnTHsKtKv9peao
"""

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

try:
    from substrateinterface import Keypair, SubstrateInterface
    from substrateinterface.contracts import ContractCode
except ImportError:
    print('ERROR: run under `uv run --with substrate-interface python ...`', file=sys.stderr)
    sys.exit(1)

HERE = Path(__file__).resolve().parent
CONTRACT = HERE / 'target' / 'ink' / 'allways_bond_vault.contract'
WASM = HERE / 'target' / 'ink' / 'allways_bond_vault.wasm'


def load_keypair_from_wallet(wallet_name: str) -> Keypair:
    """Plaintext-first, else unlock the encrypted coldkey via the wallet SDK (password prompt)."""
    coldkey = Path.home() / '.bittensor' / 'wallets' / wallet_name / 'coldkey'
    if not coldkey.exists():
        print(f'ERROR: coldkey not found: {coldkey}', file=sys.stderr)
        sys.exit(1)
    try:
        data = json.loads(coldkey.read_text())
        if 'secretSeed' in data:
            seed = data['secretSeed'][2:] if data['secretSeed'].startswith('0x') else data['secretSeed']
            return Keypair.create_from_seed(bytes.fromhex(seed))
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass
    from bittensor_wallet import Wallet

    return Wallet(name=wallet_name).coldkey  # prompts for the wallet password; non-destructive


def main():
    p = argparse.ArgumentParser(description='Deploy the allways bond vault (immutable).')
    p.add_argument('--wallet-name', required=True, help='btcli wallet whose coldkey signs+pays the deploy')
    p.add_argument('--url', required=True, help='subtensor ws endpoint')
    p.add_argument('--netuid', type=int, required=True)
    p.add_argument('--staking-hotkey', required=True, help='fee-recycle target ss58 (registered on netuid)')
    p.add_argument('--validators', required=True, help='comma-separated seed validator ss58 list')
    p.add_argument('--min-collateral', type=int, default=250_000_000, help='rao (default 0.25 τ)')
    p.add_argument('--max-collateral', type=int, default=10_000_000_000, help='rao (default 10 τ)')
    p.add_argument('--threshold', type=int, default=66, help='consensus %% (contract floor 51)')
    p.add_argument('--vote-round-ttl', type=int, default=600, help='blocks (contract floor 100)')
    p.add_argument('--yes', action='store_true', help='skip the confirmation prompt')
    args = p.parse_args()

    validators = [v.strip() for v in args.validators.split(',') if v.strip()]
    for f in (CONTRACT, WASM):
        if not f.exists():
            print(f'ERROR: missing {f} — run `cargo contract build --release` first', file=sys.stderr)
            sys.exit(1)

    print(f'== target:       {args.url} (netuid {args.netuid})')
    print(f'== recycle to:   {args.staking_hotkey}')
    print(f'== bond bounds:  min {args.min_collateral} / max {args.max_collateral} rao')
    print(f'== quorum:       {args.threshold}%, round TTL {args.vote_round_ttl} blocks')
    print(f'== seed set:     {validators}')
    if not args.yes:
        if input('Instantiate an IMMUTABLE vault with these values? [y/N] ').strip().lower() != 'y':
            print('aborted')
            sys.exit(1)

    deployer = load_keypair_from_wallet(args.wallet_name)
    print(f'Deployer: {deployer.ss58_address}')

    sub = SubstrateInterface(url=args.url)
    code = ContractCode.create_from_contract_files(metadata_file=str(CONTRACT), wasm_file=str(WASM), substrate=sub)
    salt = hashlib.sha256(str(time.time()).encode()).digest()[:4]
    try:
        contract = code.deploy(
            keypair=deployer,
            constructor='new',
            args={
                'staking_hotkey': args.staking_hotkey,
                'netuid': args.netuid,
                'min_collateral': args.min_collateral,
                'max_collateral': args.max_collateral,
                'consensus_threshold_percent': args.threshold,
                'vote_round_ttl': args.vote_round_ttl,
                'validators': validators,
            },
            value=0,
            gas_limit={'ref_time': 200_000_000_000, 'proof_size': 5_000_000},  # explicit ⇒ no dry-run
            upload_code=True,
            deployment_salt=salt,
        )
    except Exception as e:
        print(f'ERROR: deployment failed: {e}', file=sys.stderr)
        if hasattr(e, 'error_message'):
            print(f'  {e.error_message}', file=sys.stderr)
        sys.exit(1)

    addr = contract.contract_address
    print('\nVAULT DEPLOYED')
    print(f'CONTRACT_ADDRESS={addr}')
    (HERE / '.deploy').mkdir(exist_ok=True)
    (HERE / '.deploy' / 'contract_address').write_text(addr + '\n')
    print('Next: alw config set vault-address <addr>; add validators via unanimous vote + accept.')
    sub.close()


if __name__ == '__main__':
    main()

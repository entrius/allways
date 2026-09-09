"""Pure helpers behind `alw miner init`: key families, key generation, `.env` editing, wallet listing.

No chain access here — everything is unit-testable against a tmp dir.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from allways.chains import SUPPORTED_CHAINS, ChainDefinition

# Families whose key the wizard can generate itself; sol (keypair file) + tao (coldkey) are handled apart.
GENERATED_KINDS = ('evm', 'btc')


@dataclass(frozen=True)
class KeyFamily:
    """One signing key + the assets it serves, keyed by the network's env prefix (ETH, ARB, BTC…)."""

    prefix: str
    kind: str  # 'solana' | 'tao' | 'btc' | 'evm'
    assets: Tuple[str, ...]
    network_chain: Optional[ChainDefinition]  # the row that owns {PREFIX}_NETWORK, if any

    @property
    def key_env(self) -> str:
        return f'{self.prefix}_PRIVATE_KEY'

    @property
    def network_env(self) -> str:
        return f'{self.prefix}_NETWORK'

    @property
    def label(self) -> str:
        return f'{self.prefix.lower()}  ({", ".join(self.assets)})'


def _kind(chain: ChainDefinition) -> str:
    if chain.id == 'btc':
        return 'btc'
    if chain.id == 'tao':
        return 'tao'
    if chain.id == 'sol' or chain.host_chain == 'solana':
        return 'solana'
    return 'evm'


def key_families() -> List[KeyFamily]:
    """Registry-derived, in registry order: a new chain shows up here unaided."""
    by_prefix: Dict[str, List[ChainDefinition]] = {}
    for chain in SUPPORTED_CHAINS.values():
        by_prefix.setdefault(chain.env_prefix, []).append(chain)
    out = []
    for prefix, chains in by_prefix.items():
        owner = next((c for c in chains if c.networks), None)
        out.append(KeyFamily(prefix, _kind(chains[0]), tuple(c.id for c in chains), owner))
    return out


def optional_families() -> List[KeyFamily]:
    """Families the operator opts into (everything but the always-on Solana keypair + TAO coldkey)."""
    return [f for f in key_families() if f.kind in GENERATED_KINDS]


def parse_family_list(raw: str) -> List[str]:
    """'btc, eth,ARB' → ['BTC', 'ETH', 'ARB']; raises ValueError naming any unknown prefix."""
    known = {f.prefix for f in optional_families()}
    picked = [p.strip().upper() for p in raw.split(',') if p.strip()]
    unknown = [p for p in picked if p not in known]
    if unknown:
        raise ValueError(f'unknown chain(s) {", ".join(unknown)}; choose from {", ".join(sorted(known))}')
    return list(dict.fromkeys(picked))


# ─── Key generation / derivation ─────────────────────────────────────────────


def generate_evm_key() -> Tuple[str, str]:
    """(0x-prefixed 32-byte hex key, checksummed address)."""
    from eth_account import Account

    acct = Account.create()
    return '0x' + acct.key.hex().removeprefix('0x'), acct.address


def evm_address(hex_key: str) -> Optional[str]:
    from eth_account import Account

    try:
        return Account.from_key(hex_key).address
    except Exception:
        return None


def _embit_network(btc_network: str):
    from embit.networks import NETWORKS

    from allways.assets.btc import TESTNET_ENCODINGS

    if btc_network in TESTNET_ENCODINGS:
        return NETWORKS['test']
    return NETWORKS['regtest'] if btc_network == 'regtest' else NETWORKS['main']


def generate_btc_key(btc_network: str) -> Tuple[str, str]:
    """(WIF for the network, native-segwit address) — the address form the docker miner pays from."""
    from embit.ec import PrivateKey
    from embit.script import p2wpkh

    key = PrivateKey(os.urandom(32))
    net = _embit_network(btc_network)
    return key.wif(net), p2wpkh(key.get_public_key()).address(net)


def btc_address(wif: str, btc_network: str) -> Optional[str]:
    from embit.ec import PrivateKey
    from embit.script import p2wpkh

    try:
        return p2wpkh(PrivateKey.from_wif(wif).get_public_key()).address(_embit_network(btc_network))
    except Exception:
        return None


# ─── .env editing ────────────────────────────────────────────────────────────

WIZARD_SECTION = '# ─── alw miner init ────────────────────────────────────────'


def _quote(value: str) -> str:
    return f'"{value}"' if (' ' in value or '#' in value) else value


def upsert_env(text: str, values: Dict[str, str]) -> str:
    """Set each KEY=value in a dotenv text, replacing an existing (or commented-out) line in place and
    keeping its trailing comment; keys not present are appended under the wizard section."""
    lines = text.splitlines()
    pending = dict(values)
    for i, line in enumerate(lines):
        m = re.match(r'^\s*#?\s*([A-Z][A-Z0-9_]*)=.*?(\s+#.*)?$', line)
        if not m or m.group(1) not in pending:
            continue
        key = m.group(1)
        lines[i] = f'{key}={_quote(pending.pop(key))}{m.group(2) or ""}'
    if pending:
        if lines and lines[-1].strip():
            lines.append('')
        lines.append(WIZARD_SECTION)
        lines.extend(f'{k}={_quote(v)}' for k, v in pending.items())
    return '\n'.join(lines) + '\n'


def read_env(path: Path) -> Dict[str, str]:
    from dotenv import dotenv_values

    if not path.exists():
        return {}
    return {k: v for k, v in dotenv_values(path).items() if v is not None}


def write_env(path: Path, values: Dict[str, str], template: Optional[Path] = None) -> None:
    """Upsert into ``path``; a missing file starts from ``template`` (the repo's .env.example) so the
    operator keeps every documented knob and comment. Written 0600 — it holds signing keys."""
    if path.exists():
        base = path.read_text()
    elif template and template.exists():
        base = template.read_text()
    else:
        base = ''
    path.write_text(upsert_env(base, values))
    os.chmod(path, 0o600)


# ─── Bittensor wallet discovery ──────────────────────────────────────────────


def wallets_root() -> Path:
    return Path(os.environ.get('WALLET_PATH') or Path.home() / '.bittensor' / 'wallets').expanduser()


def list_wallets(root: Optional[Path] = None) -> Dict[str, List[str]]:
    """{coldkey name: [hotkey names]} for every wallet dir holding a coldkeypub.txt."""
    root = root or wallets_root()
    if not root.is_dir():
        return {}
    out: Dict[str, List[str]] = {}
    for d in sorted(root.iterdir()):
        if not (d / 'coldkeypub.txt').is_file():
            continue
        hk_dir = d / 'hotkeys'
        files = hk_dir.iterdir() if hk_dir.is_dir() else ()
        out[d.name] = sorted(p.name for p in files if p.is_file() and not p.name.endswith('pub.txt'))
    return out

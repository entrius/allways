import json
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional, Tuple

import bittensor as bt
import click
from rich.console import Console

from allways.classes import MinerPair, SwapStatus
from allways.commitments import parse_commitment_data, read_miner_commitment, read_miner_commitments  # noqa: F401
from allways.constants import CONTRACT_ADDRESS as DEFAULT_CONTRACT_ADDRESS
from allways.constants import NETUID_FINNEY, TAO_TO_RAO
from allways.contract_client import AllwaysContractClient, ContractError, is_contract_rejection

ALLWAYS_DIR = Path.home() / '.allways'
CONFIG_FILE = ALLWAYS_DIR / 'config.json'
PENDING_SWAP_FILE = ALLWAYS_DIR / 'pending_swap.json'

console = Console()

SECONDS_PER_BLOCK = 12

SWAP_STATUS_COLORS = {
    SwapStatus.ACTIVE: 'yellow',
    SwapStatus.FULFILLED: 'blue',
    SwapStatus.COMPLETED: 'green',
    SwapStatus.TIMED_OUT: 'red',
}


def loading(message: str, spinner: str = 'dots', color: str = 'cyan'):
    """Return a Rich spinner context manager for long-running operations."""
    return console.status(f'[{color}]{message}[/{color}]', spinner=spinner, spinner_style=color)


def print_contract_error(action: str, e: BaseException) -> None:
    """Print a contract error with contract-rejection vs RPC-failure distinction.

    Contract rejections (NotOwner, NotValidator, InvalidStatus, etc.) are the
    user's expected failure mode for bad state and we surface the variant
    name plainly. RPC or client-side failures get a retryable framing so the
    user knows to check connectivity rather than their input.
    """
    if isinstance(e, ContractError) and is_contract_rejection(e):
        console.print(f'[red]{action}: contract rejected — {e}[/red]')
    else:
        console.print(f'[red]{action}: {e}[/red]')
        console.print('[dim]This looks like an RPC or client failure — try again.[/dim]')


def require_confirmation(prompt: str, default: bool = False) -> bool:
    """Prompt for Y/N confirmation; print a cancel notice and return False on decline.

    Returns True only when the user accepts. Callers should early-return on False.
    """
    if not click.confirm(prompt, default=default):
        console.print('[yellow]Cancelled[/yellow]')
        return False
    return True


def is_valid_ss58(address: str) -> bool:
    """Check if a string is a syntactically valid SS58 address.

    Does not verify the account exists on-chain — only that the encoding is
    well-formed. Useful as a pre-flight guard before submitting an admin
    extrinsic whose typo would silently fail.
    """
    try:
        from scalecodec.utils.ss58 import ss58_decode

        ss58_decode(address)
        return True
    except Exception:
        return False


# Global flags that can appear anywhere in the command line.
# Maps CLI flag names to config keys.
_GLOBAL_FLAGS = {
    '--network': 'network',
    '--wallet': 'wallet',
    '--wallet.name': 'wallet',
    '--wallet-name': 'wallet',
    '--hotkey': 'hotkey',
    '--wallet.hotkey': 'hotkey',
    '--netuid': 'netuid',
}


def is_local_network(network: str) -> bool:
    """Check if the network config points to a local dev environment."""
    if network == 'local':
        return True
    return any(host in network for host in ('127.0.0.1', 'localhost', '0.0.0.0'))


def to_rao(amount_tao: float) -> int:
    """Convert TAO to rao."""
    return int(amount_tao * TAO_TO_RAO)


def from_rao(amount_rao: int) -> float:
    """Convert rao to TAO."""
    return amount_rao / TAO_TO_RAO


def load_cli_config() -> dict:
    """Load CLI configuration from ~/.allways/config.json."""
    if not CONFIG_FILE.exists():
        return {}
    try:
        return json.loads(CONFIG_FILE.read_text())
    except Exception:
        return {}


def parse_global_flags() -> dict:
    """Extract global flags (--network, --wallet, etc.) from sys.argv.

    Strips matched flags and their values from sys.argv so Click
    subcommands don't choke on unknown options.
    """
    overrides = {}
    new_argv = [sys.argv[0]]
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        # Handle --flag=value form
        if '=' in arg:
            flag, value = arg.split('=', 1)
            if flag in _GLOBAL_FLAGS:
                overrides[_GLOBAL_FLAGS[flag]] = value
                i += 1
                continue
        # Handle --flag value form
        if arg in _GLOBAL_FLAGS:
            if i + 1 < len(sys.argv):
                overrides[_GLOBAL_FLAGS[arg]] = sys.argv[i + 1]
                i += 2
                continue
        new_argv.append(arg)
        i += 1
    sys.argv[:] = new_argv
    return overrides


_CLI_OVERRIDES: dict = {}


def apply_global_flags():
    """Parse and strip global flags from sys.argv. Must be called after argv is restored."""
    global _CLI_OVERRIDES
    _CLI_OVERRIDES = parse_global_flags()


def get_effective_config() -> dict:
    """Merge file config with CLI global overrides (CLI flags win)."""
    config = load_cli_config()
    config.update(_CLI_OVERRIDES)
    return config


def get_cli_context(
    need_wallet: bool = True,
    need_client: bool = True,
) -> Tuple[dict, Optional[bt.Wallet], bt.Subtensor, Optional[AllwaysContractClient]]:
    """Standard CLI context setup: config, wallet, subtensor, contract client.

    CLI flags (--network, --wallet, --hotkey, --netuid) override config file values.
    """
    config = get_effective_config()
    network = config.get('network', 'finney')
    with console.status(
        f'[cyan]Synchronizing with chain [dim]{network}[/dim]...[/cyan]', spinner='dots', spinner_style='cyan'
    ):
        subtensor = bt.Subtensor(network=network)
        wallet = None
        if need_wallet:
            wallet = bt.Wallet(
                name=config.get('wallet', 'default'),
                hotkey=config.get('hotkey', 'default'),
            )
        contract_addr = config.get('contract-address') or config.get('contract_address') or DEFAULT_CONTRACT_ADDRESS
        client = AllwaysContractClient(contract_address=contract_addr, subtensor=subtensor) if need_client else None
    # Ensure netuid is resolved for callers
    if 'netuid' not in config:
        config['netuid'] = NETUID_FINNEY
    else:
        config['netuid'] = int(config['netuid'])
    return config, wallet, subtensor, client


# =========================================================================
# Pending swap state persistence
# =========================================================================


@dataclass
class PendingSwapState:
    miner_hotkey: str
    miner_uid: int
    from_chain: str
    to_chain: str
    from_amount: int
    to_amount: int
    tao_amount: int
    user_receives: int
    rate_str: str
    miner_from_address: str
    user_from_address: str
    receive_address: str
    reserved_until_block: int
    netuid: int
    wallet_name: str
    hotkey_name: str
    created_at: float


def save_pending_swap(state: PendingSwapState) -> None:
    """Atomically write pending swap state to ~/.allways/pending_swap.json."""
    ALLWAYS_DIR.mkdir(parents=True, exist_ok=True)
    data = json.dumps(asdict(state), indent=2)
    fd, tmp_path = tempfile.mkstemp(dir=ALLWAYS_DIR, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(data)
        os.replace(tmp_path, PENDING_SWAP_FILE)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def load_pending_swap() -> Optional[PendingSwapState]:
    """Load pending swap state. Returns None if file doesn't exist or is invalid."""
    if not PENDING_SWAP_FILE.exists():
        return None
    try:
        data = json.loads(PENDING_SWAP_FILE.read_text())
        return PendingSwapState(**data)
    except Exception:
        return None


def clear_pending_swap() -> None:
    """Remove the pending swap state file."""
    PENDING_SWAP_FILE.unlink(missing_ok=True)


def find_matching_miners(all_pairs, from_chain: str, to_chain: str):
    """Filter and normalize miner pairs for a given swap direction (bilateral matching).

    Handles both direct matches and reverse-direction pairs (using counter_rate for the
    reverse direction). Returns list of MinerPair with source/dest matching the requested
    direction. For reverse-direction matches, the returned MinerPair carries the full
    bidirectional view: `rate` is the selected-direction rate, `counter_rate` preserves
    the original canonical rate so `get_rate_for_direction` still works on the result.
    """
    matching = []
    for p in all_pairs:
        if p.from_chain == from_chain and p.to_chain == to_chain:
            if p.rate > 0:
                matching.append(p)
        elif p.from_chain == to_chain and p.to_chain == from_chain:
            rev_rate, rev_rate_str = p.get_rate_for_direction(from_chain)
            if rev_rate > 0:
                matching.append(
                    MinerPair(
                        uid=p.uid,
                        hotkey=p.hotkey,
                        from_chain=p.to_chain,
                        from_address=p.to_address,
                        to_chain=p.from_chain,
                        to_address=p.from_address,
                        rate=rev_rate,
                        rate_str=rev_rate_str,
                        counter_rate=p.rate,
                        counter_rate_str=p.rate_str,
                    )
                )
    return matching

import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import bittensor as bt
import click
import requests
from bittensor.utils import ss58_encode
from rich.console import Console
from rich.text import Text

from allways.chains import SUPPORTED_CHAINS, ChainDefinition
from allways.classes import SwapStatus
from allways.cli.swap_commands.swap_intake import backing_purse, floors_from_config
from allways.constants import NETUID_FINNEY, TAO_TO_RAO, declarable_backings
from allways.solana import pdas
from allways.solana.client import PROGRAM_ERRORS, SolanaClientError, program_error_code
from allways.solana.layouts import hub_busy_until, hub_swap_on, lock_max
from allways.solana.rpc import SolanaRpcError, SolanaRpcUnreachable, resolve_rpc_url

ALLWAYS_DIR = Path.home() / '.allways'
CONFIG_FILE = ALLWAYS_DIR / 'config.json'
PENDING_SWAP_FILE = ALLWAYS_DIR / 'pending_swap.json'

# ─── Per-chain network resolution ────────────────────────────────────────────
# Each chain takes a simple network NAME (alw config set solana-network devnet); the code
# maps it to an endpoint so operators never hand-copy RPC URLs. Raw-URL escape hatches still
# win for paid/custom RPCs (SOLANA_RPC_URL env / solana-rpc config; BTC_ESPLORA_URLS env).
# The Solana SIGNER resolves the same way: SOLANA_KEYPAIR_PATH env wins, else the
# solana-keypair config path, else the solana-CLI default ~/.solana/id.json.
SOLANA_NETWORKS = {
    'devnet': 'https://api.devnet.solana.com',
    'mainnet': 'https://api.mainnet-beta.solana.com',
    'localnet': 'http://127.0.0.1:8899',
}
# Chains that pick their network by NAME from {env_prefix}_NETWORK (registry rows carry the
# accepted names). Every per-chain CLI surface — the `<id>-network` keys, the env bundles, the
# `alw config` rows, `config set` validation — derives from this, so a new chain adds none.
NAME_SELECTED_CHAINS = tuple(c for c in SUPPORTED_CHAINS.values() if c.networks)


def network_key(chain: ChainDefinition) -> str:
    """CLI config key that sets this NETWORK's name (e.g. 'btc-network', 'arb-network').

    Keyed off env_prefix, not the asset id, so it names the same thing the env var does —
    assets sharing a network get one row between them, never one each."""
    return f'{chain.env_prefix.lower()}-network'


def testnet_name(chain: ChainDefinition) -> str:
    """Network the `env testnet` bundle picks; a chain with no testnet stays on its default."""
    return chain.testnet_network or chain.networks[0]


CHAIN_NETWORK_KEYS = tuple(network_key(c) for c in NAME_SELECTED_CHAINS)
# One-liner env bundle: `alw config set env testnet|mainnet` sets every chain's network + netuid
# + the default router. Testnet routes through the Ventura Labs validator; mainnet self-represents
# (no routing validator live yet). `alw config set router <ss58>` opts into routing on mainnet.
ENV_BUNDLES = {
    'testnet': {
        'network': 'test',
        'solana-network': 'devnet',
        **{network_key(c): testnet_name(c) for c in NAME_SELECTED_CHAINS},
        'netuid': '19',
        'router': '5HicmHG7fjbxrtx8FZNdv4xxS5jSN84KGpMnTHsKtKv9peao',
    },
    'mainnet': {
        'network': 'finney',
        'solana-network': 'mainnet',
        **{network_key(c): c.networks[0] for c in NAME_SELECTED_CHAINS},
        'netuid': '7',
        # No routing validator on mainnet yet — bid self-represented until one ships a routing
        # product. Explicit '' (not omitted) so re-running `env mainnet` CLEARS a stale router.
        'router': '',
    },
}


def resolve_solana_rpc(config: dict) -> str:
    """Solana RPC precedence: SOLANA_RPC_URL env / solana-rpc config (raw URL — paid/custom) win;
    else the solana-network name resolves to a public endpoint; else localnet default.
    ``SOLANA_RPC_API_KEY`` is composed onto whichever endpoint wins (``resolve_rpc_url``)."""
    url = os.environ.get('SOLANA_RPC_URL') or config.get('solana-rpc')
    name = config.get('solana-network')
    if not url and name:
        url = SOLANA_NETWORKS.get(name)
        if not url:
            console.print(
                f'[yellow]Unknown solana-network {name!r} (expected {list(SOLANA_NETWORKS)}); using localnet.[/yellow]'
            )
    return resolve_rpc_url(url)


def resolve_solana_keypair_path(config: dict) -> str:
    """Solana keypair precedence: SOLANA_KEYPAIR_PATH env (raw path escape hatch) wins;
    else the solana-keypair config path; else the solana-CLI default ~/.solana/id.json."""
    raw = os.environ.get('SOLANA_KEYPAIR_PATH') or config.get('solana-keypair')
    if raw:
        return os.path.expanduser(raw)
    return str(Path.home() / '.solana' / 'id.json')


def load_cli_keypair(config: dict):
    """Load the CLI signing keypair from the resolved path (see resolve_solana_keypair_path).

    An explicitly pointed-at path (env/config) must exist — minting a fresh key there would
    silently sign with an unfunded, non-authority identity. Only the bare ~/.solana/id.json
    default keeps the auto-generate convenience."""
    from allways.solana import keys

    path = resolve_solana_keypair_path(config)
    explicit = os.environ.get('SOLANA_KEYPAIR_PATH') or config.get('solana-keypair')
    if explicit and not os.path.exists(path):
        fail(
            f'Configured solana-keypair {path} not found (from SOLANA_KEYPAIR_PATH env or `alw config set solana-keypair`).'
        )
    return keys.load_or_create(path)


def apply_chain_network_env(config: dict) -> None:
    """Feed each `<id>-network` config value into the provider's {PREFIX}_NETWORK env var.
    A real env var wins (explicit override); otherwise the configured name is applied."""
    for chain in NAME_SELECTED_CHAINS:
        env_var = f'{chain.env_prefix}_NETWORK'
        if not os.environ.get(env_var) and config.get(network_key(chain)):
            os.environ[env_var] = config[network_key(chain)]


# Quote-update churn fee tiers — mirror smart-contracts/…/constants.rs quote_update_fee().
QUOTE_UPDATE_FEE_TIERS = ((300, 10_000_000), (600, 1_000_000))  # (elapsed < secs, lamports); else free


def quote_update_fee_lamports(elapsed_secs: int) -> int:
    """Churn fee (lamports) to re-quote a direction ``elapsed_secs`` after its last update: 0.01 SOL
    under 5 min, 0.001 SOL at 5–10 min, free after 10 min. Creation is free. Mirrors the contract."""
    for below, fee in QUOTE_UPDATE_FEE_TIERS:
        if elapsed_secs < below:
            return fee
    return 0


def votes_needed(cfg) -> int:
    """Votes required for consensus — mirrors the program's headcount check
    (consensus.rs: votes*100 >= threshold*total), i.e. ceil(threshold*total/100)."""
    total = len(cfg.validators)
    return -(-cfg.consensus_threshold_percent * total // 100)


console = Console()


class CliError(Exception):
    """Local CLI/Solana error type — replaces the deleted ink! contract error."""


# When a --json command is running, errors must stay machine-readable — `fail()` emits `{"error": ...}`
# instead of Rich text so a consumer piping --json never chokes on a plain-text error. Set per command.
_JSON_OUTPUT = False


def set_json_output(enabled: bool) -> None:
    """Mark the current command as JSON-mode so `fail()` (and thus `safe_read`) emit JSON errors."""
    global _JSON_OUTPUT
    _JSON_OUTPUT = bool(enabled)


class FiniteFloatType(click.ParamType):
    """A float option that rejects nan/inf at parse time with a clean Click usage error — so
    user-supplied `--amount nan/inf/1e999` never reaches an `int()` cast and dumps a traceback."""

    name = 'number'

    def convert(self, value, param, ctx):
        try:
            f = float(value)
        except (TypeError, ValueError):
            self.fail(f'{value!r} is not a number', param, ctx)
        if not math.isfinite(f):
            self.fail(f'{value!r} must be a finite number (not nan/inf)', param, ctx)
        return f


FINITE_FLOAT = FiniteFloatType()


class FiniteDecimalType(click.ParamType):
    """A finite decimal for asset amounts that must retain every entered digit."""

    name = 'decimal'

    def convert(self, value, param, ctx):
        try:
            number = Decimal(str(value))
        except (InvalidOperation, ValueError):
            self.fail(f'{value!r} is not a decimal number', param, ctx)
        if not number.is_finite():
            self.fail(f'{value!r} must be a finite number (not nan/inf)', param, ctx)
        return number


FINITE_DECIMAL = FiniteDecimalType()


def fail(message: str, code: int = 1) -> None:
    """Single error-exit path: exit non-zero so `$?` reflects failure. In JSON mode emits
    `{"error": ...}`; otherwise prints the message in red.

    Every command rejection/error across the CLI routes through here — that is what makes the CLI script-safe."""
    if _JSON_OUTPUT:
        click.echo(json.dumps({'error': message}))
    else:
        console.print(f'[red]{message}[/red]')
    raise SystemExit(code)


# --- Pending swap context (taker reserve → post-tx handoff) ---------------
# `alw swap now` stashes the just-reserved miner pubkey via `_save_pending` (see swap.py). That gives
# `alw swap post-tx` a fast, unambiguous handle on the winning miner; post-tx re-validates it against
# the chain and falls back to scanning all bindings if it's missing (e.g. run from another machine).
# The confirm relay is keyed by the miner's Bittensor hotkey (reservations are keyed by miner), so
# post-tx resolves the pubkey → hotkey via the on-chain binding. Non-authoritative cache — safe to delete.
def hotkey_bytes_to_ss58(hotkey: bytes) -> str:
    """32-byte sr25519 public key → ss58 (Bittensor format 42). Empty on bad input."""
    try:
        return ss58_encode(bytes(hotkey), ss58_format=42)
    except Exception:
        return ''


def load_pending_swap() -> Optional[dict]:
    """Read the stashed swap context, or None if absent/unreadable."""
    try:
        return json.loads(PENDING_SWAP_FILE.read_text())
    except Exception:
        return None


def clear_pending_swap() -> None:
    """Remove the stashed context once the confirm relay is accepted."""
    try:
        PENDING_SWAP_FILE.unlink()
    except Exception:
        pass


EMPTY_SWAP_KEY = bytes(32)


def live_unclaimed(resv) -> bool:
    """Shared 'usable reservation' predicate for BOTH origination (`swap now`) and confirm (`post-tx`),
    so the two paths can never disagree about what counts as a resolved reservation.

    A reservation must exist, still be within its TTL (``reserved_until > now``), and carry no claim yet
    (empty ``claimed_swap_key``). Crucially, ``reserved_until != 0`` alone is NOT sufficient: a reservation
    left over from an abandoned reserve keeps its non-zero-but-past ``reserved_until`` indefinitely (nothing
    reaps it on-chain), and treating that stale value as 'the draw resolved' is what let `swap now` tell a
    taker to send funds before the pool draw ran — an unrecoverable loss."""
    if resv is None:
        return False
    now = int(time.time())
    return int(resv.reserved_until) > now and bytes(resv.claimed_swap_key) == EMPTY_SWAP_KEY


# Operator next step per Anchor code where the IDL message alone is not actionable from the CLI.
PROGRAM_ERROR_HINTS = {
    6002: 'Deposit collateral first: alw collateral deposit',
    6003: 'Deactivate before withdrawing: alw miner deactivate',
    6015: 'Activate first: alw miner activate',
}


def solana_failure_message(err: Exception) -> str:
    """One actionable line for a program/RPC failure — never the raw RPC dict or a traceback."""
    code = program_error_code(err)
    if code is not None:
        name, msg = PROGRAM_ERRORS.get(code, ('UnknownError', 'Program rejected the transaction'))
        return f'{PROGRAM_ERROR_HINTS.get(code, msg)} ({name} {code})'
    rpc_message = re.search(r"'message': '([^']*)'", str(err))
    return rpc_message.group(1) if rpc_message else str(err)


def print_json(data) -> None:
    """Emit a value as pretty JSON (str fallback for non-serializable types like Pubkey)."""
    click.echo(json.dumps(data, indent=2, default=str))


def safe_read(fn: Callable, what: str = 'read from Solana'):
    """Run a client read, converting any RPC/decode/transport failure into a clean non-zero `fail`.

    Reads raise `SolanaClientError` (decode), `SolanaRpcError` (RPC-level error), or a bare
    `requests` transport error (unreachable RPC). All three must surface as a script-safe failure,
    never a stacktrace."""
    try:
        return fn()
    except SolanaRpcUnreachable as e:
        hint = (
            ''
            if CONFIG_FILE.exists()
            else ' No config file found — run `alw config set env testnet` (or `mainnet`) first.'
        )
        fail(f'Could not reach the Solana RPC at {e.url} to {what}.{hint}')
    except (SolanaClientError, SolanaRpcError) as e:
        fail(f'Failed to {what}: {e}')
    except requests.RequestException:
        fail(f'Could not reach the Solana RPC to {what}. Is the node up at the configured solana-rpc?')


ZERO_SWAP_KEY = bytes(32)


@dataclass
class MinerBookEntry:
    """One miner's aggregated on-chain view: its posted quotes (one per direction) plus runtime state.

    Taker views key by the Solana miner pubkey (a miner has no bittensor uid here), so `miner` is the identity."""

    miner: object  # solders Pubkey
    quotes: List[object] = field(default_factory=list)  # MinerQuote rows, one per direction
    collateral: int = 0  # lamports
    state: Optional[object] = None  # MinerState | None
    reservation: Optional[object] = None  # Reservation | None

    @property
    def pubkey_str(self) -> str:
        return str(self.miner)


def load_miner_book(client, with_reservation: bool = True) -> List[MinerBookEntry]:
    """Group all on-chain MinerQuote rows by miner and attach collateral + state (+ reservation).

    One `MinerBookEntry` per distinct miner pubkey; `quotes` holds its per-direction rows. Any read failure
    routes through `safe_read` (clean non-zero exit)."""
    rows = safe_read(lambda: client.get_all('MinerQuote'), what='read miner quotes')
    by_miner: dict = {}
    for _pk, q in rows:
        by_miner.setdefault(bytes(q.miner), MinerBookEntry(miner=q.miner)).quotes.append(q)
    book = list(by_miner.values())
    for entry in book:
        entry.collateral = (
            safe_read(lambda m=entry.miner: client.get_collateral_lamports(m), what='read collateral') or 0
        )
        entry.state = safe_read(lambda m=entry.miner: client.get_miner_state(m), what='read miner state')
        if with_reservation:
            entry.reservation = safe_read(
                lambda m=entry.miner: freshest_reservation(client, m), what='read reservation'
            )
    return book


def freshest_reservation(client, miner):
    """The most-alive reservation across the miner's per-hub slots (v3.1) — the one whose hold or
    finalize window reaches furthest, so status views don't miss a tao-hub seat."""
    best, best_at = None, -1
    for hub in pdas.BACKING_BITS:
        r = client.get_reservation(miner, hub)
        if r is None:
            continue
        at = max(int(getattr(r, 'reserved_until', 0) or 0), int(getattr(r, 'finalize_by', 0) or 0))
        if at > best_at:
            best, best_at = r, at
    return best


def miner_runtime_status(state, reservation, now: int, backing: Optional[str] = None) -> str:
    """Collapse on-chain miner state into one runtime label the taker views sort/filter on.

    With `backing`, the offline/in-swap/cooldown labels read THAT hub's own state (v3.1), so a SOL
    swap never paints a still-free TAO purse busy; without it, the OR view for the whole miner."""
    if state is None or not state.active:
        return 'offline'
    bit = pdas.BACKING_BITS.get(backing) if backing else None
    if bit is not None and not int(getattr(state, 'active_backings', 0) or 0) & bit:
        return 'offline'
    in_swap = hub_swap_on(state, bit) if bit is not None else state.has_active_swap
    busy_until = hub_busy_until(state, bit) if bit is not None else lock_max(getattr(state, 'busy_until', 0))
    if in_swap:
        return 'in-swap'
    if (
        reservation is not None
        and int(getattr(reservation, 'reserved_until', 0)) > now
        and bytes(getattr(reservation, 'claimed_swap_key', ZERO_SWAP_KEY)) == ZERO_SWAP_KEY
    ):
        return 'reserved'
    if busy_until > now:
        return 'cooldown'
    return 'available'


STATUS_STYLES = {
    'available': 'green',
    'reserved': 'cyan',
    'in-swap': 'yellow',
    'cooldown': 'magenta',
    'offline': 'dim',
}
# Deterministic sort order for `--sort status`: most-available first.
STATUS_SORT_ORDER = ['available', 'reserved', 'in-swap', 'cooldown', 'offline']


# --- Miner reliability (swap success rate) -------------------------------
# Per-miner success rate is not on-chain. `view rates` and `swap now` pull a
# pre-aggregated per-direction completed/total map from the allways API and
# color-code it. Override the host with ALLWAYS_API_URL for testnet or a
# self-hosted indexer.
DEFAULT_API_URL = 'https://api.all-ways.io'
RELIABILITY_CACHE_TTL = 600  # seconds — stats move slowly; avoid refetching every call


def _api_url() -> str:
    return os.environ.get('ALLWAYS_API_URL', DEFAULT_API_URL).rstrip('/')


def fetch_miner_reliability(use_cache: bool = True) -> Optional[dict]:
    """Per-miner, per-direction swap success counts from the allways API.

    Returns ``{hotkey: {'btc->tao': (completed, total), ...}}`` from
    ``/miners/reliability`` — resolved swaps only (COMPLETED + TIMED_OUT) over
    the API's credibility window. Returns ``None`` if the API is unreachable:
    callers must degrade gracefully, since `view rates` and `swap now` have to
    work whether or not the indexer is up.
    """
    cache_file = ALLWAYS_DIR / 'miner_reliability_cache.json'
    api_url = _api_url()
    if use_cache and cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text())
            fresh = time.time() - cached.get('fetched_at', 0) < RELIABILITY_CACHE_TTL
            # A cache from a different API host must not be reused.
            if fresh and cached.get('api_url') == api_url:
                return {hk: {d: tuple(v) for d, v in dirs.items()} for hk, dirs in cached['stats'].items()}
        except (json.JSONDecodeError, KeyError, OSError):
            pass  # stale/corrupt cache — fall through and refetch

    # The API rejects unknown user agents; identify ourselves explicitly.
    headers = {'User-Agent': f'allways-cli/{__import__("allways").__version__}'}
    try:
        resp = requests.get(f'{api_url}/miners/reliability', headers=headers, timeout=10)
        resp.raise_for_status()
        rows = resp.json()
    except (requests.RequestException, ValueError):
        return None
    # A JSON error object (dict) instead of a list means no usable data.
    if not isinstance(rows, list):
        return {}

    stats: dict = {}
    for r in rows:
        hk = r.get('minerHotkey')
        src = r.get('sourceChain')
        dst = r.get('destChain')
        if not hk or not src or not dst:
            continue
        stats.setdefault(hk, {})[f'{src}->{dst}'] = (int(r.get('completed') or 0), int(r.get('total') or 0))

    try:
        ALLWAYS_DIR.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(
            json.dumps(
                {
                    'fetched_at': int(time.time()),
                    'api_url': api_url,
                    'stats': {hk: {d: list(v) for d, v in dirs.items()} for hk, dirs in stats.items()},
                }
            )
        )
    except OSError:
        pass  # cache write is best-effort
    return stats


def reliability_text(hotkey: str, src: str, dst: str, reliability: Optional[dict]) -> Text:
    """Colored ``completed/total`` for one swap direction.

    Green ≥90%, yellow ≥50%, red below; dim ``—`` when reliability is
    unavailable or the miner has no resolved swap in that direction.
    """
    if reliability is None:
        return Text('—', style='dim')
    comp, tot = reliability.get(hotkey, {}).get(f'{src}->{dst}', (0, 0))
    if tot == 0:
        return Text('—', style='dim')
    pct = comp / tot
    style = 'green' if pct >= 0.9 else 'yellow' if pct >= 0.5 else 'red'
    return Text(f'{comp}/{tot}', style=style)


SWAP_STATUS_COLORS = {
    SwapStatus.ACTIVE: 'yellow',
    SwapStatus.FULFILLED: 'blue',
    SwapStatus.COMPLETED: 'green',
    SwapStatus.TIMED_OUT: 'red',
}


def loading(message: str, spinner: str = 'dots', color: str = 'cyan'):
    """Return a Rich spinner context manager for long-running operations."""
    return console.status(f'[{color}]{message}[/{color}]', spinner=spinner, spinner_style=color)


def sign_or_prompt_external(
    provider,
    address: str,
    message: str,
    key=None,
    chain: str = '',
    skip_confirm: bool = False,
) -> str:
    """Sign a proof-of-ownership message, falling back to externally-pasted signature.

    Tries internal signing first (env var WIF, wallet coldkey).
    On failure for BTC source swaps in interactive mode, prompts the user to
    sign the exact message in an external wallet (Electrum, Sparrow, Trezor,
    Bitcoin Core) and paste the base64 BIP-137 signature. Verifies the pasted
    signature before returning it so a typo fails here rather than at the
    validator.

    Returns an empty string when no valid signature is obtained.
    """
    try:
        signature = provider.chain.sign_from_proof(address, message, key)
    except Exception as e:
        bt.logging.warning(f'Internal signing failed ({type(e).__name__}): {e}')
        signature = ''

    if signature:
        return signature

    if skip_confirm or chain != 'btc':
        return ''

    console.print('\n  [bold yellow]External signature required[/bold yellow]')
    console.print(
        '  [dim]No BTC signing key loaded. Sign the message below in your wallet\n'
        '  (Electrum: Tools -> Sign/verify message; Sparrow, Trezor, Bitcoin Core\n'
        '  all support this) and paste the base64 signature back.[/dim]'
    )
    console.print(f'\n  Address: [cyan]{address}[/cyan]')
    console.print(f'  Message: [yellow]{message}[/yellow]\n')

    pasted = click.prompt('  Paste signature (blank to cancel)', default='', show_default=False).strip()
    if not pasted:
        return ''

    try:
        verified = provider.chain.verify_from_proof(address, message, pasted)
    except Exception as e:
        console.print(f'[red]Signature verification errored: {e}[/red]')
        return ''

    if not verified:
        console.print(
            '[red]Signature did not verify for this address/message. Make sure you signed the exact\n'
            'message shown above with the private key for that address.[/red]'
        )
        return ''

    console.print('[green]  Signature verified.[/green]')
    return pasted


def is_valid_ss58(address: str) -> bool:
    """Check if a string is a syntactically valid SS58 address.

    Does not verify the account exists on-chain — only that the encoding is
    well-formed. Useful as a pre-flight guard before submitting an admin
    extrinsic whose typo would silently fail.
    """
    try:
        from bittensor.utils import ss58_decode

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


PROD_DASHBOARD_URL = 'https://all-ways.io'
TEST_DASHBOARD_URL = 'https://test.all-ways.io'


def dashboard_url(network: Optional[str] = None) -> str:
    """Resolve the dashboard base URL for the active network.

    finney maps to the mainnet dashboard; every other network (test, local,
    custom endpoints) maps to the testnet dashboard. ALLWAYS_DASHBOARD_URL
    overrides everything for staging/local use.
    """
    override = os.environ.get('ALLWAYS_DASHBOARD_URL')
    if override:
        return override.rstrip('/')
    if network is None:
        network = get_effective_config().get('network', 'finney')
    return (PROD_DASHBOARD_URL if network == 'finney' else TEST_DASHBOARD_URL).rstrip('/')


def to_rao(amount_tao: float) -> int:
    """Convert TAO to rao."""
    return int(amount_tao * TAO_TO_RAO)


def from_rao(amount_rao: int) -> float:
    """Convert rao to TAO."""
    return amount_rao / TAO_TO_RAO


LAMPORTS_PER_SOL = 1_000_000_000


def to_lamports(amount_sol: float) -> int:
    """Convert SOL to lamports."""
    return int(amount_sol * LAMPORTS_PER_SOL)


def from_lamports(amount_lamports: int) -> float:
    """Convert lamports to SOL."""
    return amount_lamports / LAMPORTS_PER_SOL


def secs_str(secs: int) -> str:
    """Render a seconds duration for admin setters + view dumps: bare `45s` under a minute, `600s (~10m)` above."""
    if secs < 60:
        return f'{secs}s'
    return f'{secs}s (~{secs // 60}m)'


def get_solana_cli_context(need_keypair: bool = True):
    """Solana CLI setup for the B4-repointed miner/admin commands → (config, solana_client).

    The miner/admin identity is the Solana keypair (SOLANA_KEYPAIR_PATH env / solana-keypair config /
    ~/.solana/id.json), NOT the bt wallet — collateral, quotes, and config are keyed by that pubkey on the
    program. The bt wallet is only needed where a command links the two identities (`alw miner bind-hotkey`).
    """
    from allways.solana.client import AllwaysSolanaClient
    from allways.solana.program import resolve_program_id

    config = get_effective_config()
    rpc_url = resolve_solana_rpc(config)
    program_id = resolve_program_id(config)
    keypair = load_cli_keypair(config) if need_keypair else None
    return config, AllwaysSolanaClient(rpc_url, program_id=program_id, keypair=keypair)


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
    need_client: bool = False,
) -> Tuple[dict, Optional[bt.Wallet], bt.Subtensor, None]:
    """Standard bt-side CLI context: config, wallet, subtensor (no contract client).

    The ink! contract client is gone (B6); the 4th tuple slot stays ``None`` so the
    bt-wallet callers keep their unpacking. ``need_client`` is accepted for call-site
    compatibility but no longer builds anything."""
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
    # Ensure netuid is resolved for callers
    if 'netuid' not in config:
        config['netuid'] = NETUID_FINNEY
    else:
        config['netuid'] = int(config['netuid'])
    return config, wallet, subtensor, None


# ─── Quote backing (W2b / D2) ────────────────────────────────────────────────
# A quote declares which purse answers for it. Which backings a given quote MAY declare is fixed by
# the pair (hub-capable AND one of the legs); which the miner may declare is fixed by its own
# activation mask. The intersection is what these helpers resolve.

BACKING_LABELS = {'sol': 'sol-backed', 'tao': 'tao-backed'}


def backing_label(backing: Optional[str]) -> str:
    """How a quote's backing reads in a listing. An unknown id is shown verbatim rather than hidden —
    a quote the CLI doesn't recognize is exactly the thing an operator needs to see."""
    if not backing:
        return 'unbacked'
    return BACKING_LABELS.get(backing, f'{backing}-backed')


# declarable_backings lifted to allways.constants (F4: scoring keys its lanes off it too);
# re-imported above so the CLI call sites and `from helpers import declarable_backings` keep working.


def resolve_quote_backing(miner_state, from_chain: str, to_chain: str, explicit: Optional[str] = None) -> str:
    """Which backing a `set_quote` should declare, per the D2 ergonomics.

    Infers SILENTLY when exactly one of the miner's purses qualifies (the common case), and HARD-ERRORS
    naming --backing when both do. It never breaks the tie from market state: the backing changes the
    guarantee a taker gets, so a scripted `alw miner post` must mean the same thing every time it runs.
    """
    pair = declarable_backings(from_chain, to_chain)
    if not pair:
        fail(
            f'{from_chain}->{to_chain} has no hub leg, so no purse can back it. '
            f'One leg must be a collateral chain ({", ".join(pdas.BACKING_BITS)}).'
        )
    mask = int(getattr(miner_state, 'active_backings', 0) or 0) if miner_state is not None else 0
    active = [b for b in pair if mask & pdas.BACKING_BITS[b]]

    if explicit is not None:
        explicit = explicit.lower()
        if explicit not in pdas.BACKING_BITS:
            fail(f'--backing must be one of: {", ".join(pdas.BACKING_BITS)} (got "{explicit}")')
        if explicit not in pair:
            fail(
                f'--backing {explicit} is not a leg of {from_chain}->{to_chain}. '
                f'This pair can be backed by: {", ".join(pair)}.'
            )
        if explicit not in active:
            fail(
                f'Your {explicit.upper()} purse is not active, so it cannot back a quote. '
                f'Activate it first (`alw miner activate`), then post.'
            )
        return explicit

    if not active:
        fail(
            f'No active purse can back {from_chain}->{to_chain}. '
            f'This pair needs one of: {", ".join(pair)} — activate that purse first '
            f'(`alw miner activate`), then post.'
        )
    if len(active) > 1:
        fail(
            f'{from_chain}->{to_chain} can be backed by either of your active purses '
            f'({", ".join(active)}), so the choice is yours to make: pass --backing '
            f'<{"|".join(active)}>. The backing sets the failure guarantee a taker gets '
            f'(SOL = instant SOL refund; TAO = TAO reimbursement, shortly after timeout), '
            f'so it is never inferred from market depth.'
        )
    return active[0]


# ─── Activation backing (W2 / D2) ────────────────────────────────────────────
# Purses activate one at a time, so `alw miner activate` names one. A purse is a candidate when it
# is DARK and funded above its own floor — the same purse read and floor the contract's guard uses,
# so the CLI's answer about which purses are ready is the chain's answer.


@dataclass(frozen=True)
class PurseState:
    """One backing's activation-relevant facts, in that backing's own smallest unit."""

    backing: str
    purse: Optional[int]  # None = nothing usable there: no bond at all, or one that isn't locked
    floor: int
    lit: bool

    @property
    def ready(self) -> bool:
        return not self.lit and self.purse is not None and self.purse >= self.floor


def purse_states(client, miner, miner_state, config) -> List[PurseState]:
    """Every backing this subnet knows, as it stands for this miner. Drives both the activation
    choice and `alw miner status`, so the two can never disagree about why a purse isn't serving."""
    mask = int(getattr(miner_state, 'active_backings', 0) or 0) if miner_state is not None else 0
    floors = floors_from_config(config) if config is not None else {}
    out = []
    for backing, bit in pdas.BACKING_BITS.items():
        purse = backing_purse(client, miner, miner_state, backing) if miner_state is not None else None
        out.append(PurseState(backing, purse, int(floors.get(backing, 0)), bool(mask & bit)))
    return out


def resolve_activation_backing(states: List[PurseState], explicit: Optional[str] = None) -> str:
    """Which purse `alw miner activate` should ask validators to light, per the D2 ergonomics.

    Infers SILENTLY when exactly one purse is dark and funded (the common case — including the miner
    who bonds TAO after activating SOL), and HARD-ERRORS naming --backing when both are. Funding is
    never the tie-breaker between two ready purses: the miner says which guarantee it wants to sell.
    """
    by_backing = {s.backing: s for s in states}
    ready = [s.backing for s in states if s.ready]

    if explicit is not None:
        explicit = explicit.lower()
        state = by_backing.get(explicit)
        if state is None:
            fail(f'--backing must be one of: {", ".join(pdas.BACKING_BITS)} (got "{explicit}")')
        if state.lit:
            fail(f'Your {explicit.upper()} purse is already serving.')
        if not state.ready:
            fail(_underfunded(state))
        return explicit

    if not ready:
        dark = [s for s in states if not s.lit]
        if not dark:
            fail('Every purse you have is already serving.')
        fail(' '.join(_underfunded(s) for s in dark))
    if len(ready) > 1:
        fail(
            f'Both of your purses are funded and dark ({", ".join(ready)}), so the choice is yours to '
            f'make: pass --backing <{"|".join(ready)}>. Each purse sells a different failure guarantee '
            f'(SOL = instant SOL refund; TAO = TAO reimbursement, shortly after timeout), and you can '
            f'activate the other one straight after.'
        )
    return ready[0]


def activation_prerequisites(backing: str) -> List[str]:
    """What the named purse needs before validators will vote for it. A SOL checklist shown to a
    miner whose TAO activation was refused is worse than no checklist: it sends them to the wrong
    chain to fix the wrong thing."""
    if backing == pdas.BACKING_CHAIN_SOL:
        return ['Collateral posted (alw collateral deposit) — activation gates on the purse, not on quotes']
    return [
        f'{backing.upper()} bond posted AND locked in the vault (alw vault post-collateral, alw vault lock)',
        'Validators have mirrored that bond to Solana — the attestation is written on their cadence,'
        ' so a fresh lock needs a minute',
    ]


def _underfunded(state: PurseState) -> str:
    """Why a dark purse is not a candidate — the shortfall and its fix, not a generic 'not eligible'."""
    if state.purse is None:
        return (
            f'Your {state.backing.upper()} purse has no LOCKED bond attested on Solana yet '
            f'(`alw vault post-collateral` then `alw vault lock`, then give validators a minute).'
        )
    fix = 'alw collateral deposit' if state.backing == pdas.BACKING_CHAIN_SOL else 'alw vault post-collateral'
    return f'Your {state.backing.upper()} purse holds {state.purse} < the {state.floor} floor (`{fix}`).'

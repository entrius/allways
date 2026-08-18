"""V-M3: a testnet neuron with an explicit mainnet SOLANA_RPC_URL silently ran the hub on mainnet.

The old guard was a log-only ``'mainnet' in url`` check — blind to real paid endpoints (Helius /
QuickNode subdomains, bare IPs, ``?api-key=`` URLs), which is exactly what a mainnet RPC looks like.
The boot guard now classifies the cluster *positively* by genesis hash and fails closed.
"""

from types import SimpleNamespace

import pytest

from allways.constants import NETUID_FINNEY
from allways.solana.rpc import ALLOW_MAINNET_ON_TESTNET_ENV, assert_cluster_safe, classify_cluster

MAINNET = '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d'
TESTNET = '4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY'
PROGRAM = '6JVBEj5w27J2SVjERmv2c7wXgFee9nSSBKUJevHehyBD'
TESTNET_NEURON = NETUID_FINNEY + 100  # any netuid != finney


def fake_rpc(genesis, program_exists=True, url='https://rpc.helius.example/?api-key=secret'):
    """An RPC stub with only what the guard touches. genesis/exists may be an Exception to raise."""

    def get_genesis_hash():
        if isinstance(genesis, Exception):
            raise genesis
        return genesis

    def get_account_info(_pubkey):
        if isinstance(program_exists, Exception):
            raise program_exists
        return b'\x00' if program_exists else None

    return SimpleNamespace(url=url, get_genesis_hash=get_genesis_hash, get_account_info=get_account_info)


@pytest.fixture(autouse=True)
def _clear_override(monkeypatch):
    monkeypatch.delenv(ALLOW_MAINNET_ON_TESTNET_ENV, raising=False)


def test_classify_cluster_reads_genesis_not_url():
    assert classify_cluster(MAINNET) == 'mainnet'
    assert classify_cluster(TESTNET) == 'testnet'
    assert classify_cluster('deadbeef') == 'unknown'  # localnet / custom validator


def test_testnet_neuron_on_mainnet_fails_closed():
    # The finding: a keyed mainnet URL with no literal 'mainnet' substring still gets caught.
    with pytest.raises(RuntimeError, match='MAINNET'):
        assert_cluster_safe(fake_rpc(MAINNET), PROGRAM, TESTNET_NEURON)


def test_override_env_bypasses_the_guard(monkeypatch):
    monkeypatch.setenv(ALLOW_MAINNET_ON_TESTNET_ENV, '1')
    assert_cluster_safe(fake_rpc(MAINNET), PROGRAM, TESTNET_NEURON)  # no raise


def test_mainnet_neuron_on_mainnet_is_fine():
    assert_cluster_safe(fake_rpc(MAINNET, program_exists=True), PROGRAM, NETUID_FINNEY)


def test_program_absent_on_known_cluster_is_a_mismatch():
    with pytest.raises(RuntimeError, match='does not exist'):
        assert_cluster_safe(fake_rpc(TESTNET, program_exists=False), PROGRAM, TESTNET_NEURON)


def test_program_present_on_matching_cluster_passes():
    assert_cluster_safe(fake_rpc(TESTNET, program_exists=True), PROGRAM, TESTNET_NEURON)


def test_unknown_cluster_skips_program_check():
    # Localnet dev: the program may not be deployed yet — never crash boot over it.
    assert_cluster_safe(fake_rpc('localnet-hash', program_exists=False), PROGRAM, TESTNET_NEURON)


def test_unreachable_rpc_only_warns():
    # A boot-time transport hiccup must not crash the process — the guard is best-effort against
    # transient faults, hard only against a positively identified mainnet.
    assert_cluster_safe(fake_rpc(ConnectionError('every endpoint down')), PROGRAM, TESTNET_NEURON)


def test_program_probe_failure_does_not_crash():
    assert_cluster_safe(fake_rpc(TESTNET, program_exists=ConnectionError('probe timeout')), PROGRAM, TESTNET_NEURON)

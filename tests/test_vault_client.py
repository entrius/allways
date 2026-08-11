"""The shared bond-vault client: SCALE encoding, ink! metadata, and call classification.

The vault is frozen and immutable (D7), so its wire format is a fixed target — these tests pin the
encoder against the real build artifact rather than against our own idea of it.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from allways.vault import BondVaultClient, VaultConfigError, codec
from allways.vault.client import resolve_metadata_path, resolve_vault_address

METADATA = (
    Path(__file__).resolve().parents[1]
    / 'smart-contracts'
    / 'ink-bond-vault'
    / 'target'
    / 'ink'
    / 'allways_bond_vault.json'
)

ALICE = codec.account_ss58(bytes([1] * 32))
BOB = codec.account_ss58(bytes([2] * 32))


@pytest.fixture(scope='module')
def meta():
    if not METADATA.exists():
        pytest.skip('vault metadata artifact not built')
    return codec.VaultMetadata.from_path(METADATA)


# --- encoding -----------------------------------------------------------------------------------


def test_compact_lengths_match_scale():
    assert codec.compact(0) == b'\x00'
    assert codec.compact(1) == b'\x04'
    assert codec.compact(63) == b'\xfc'
    assert codec.compact(64) == b'\x01\x01'
    assert codec.compact(255) == b'\xfd\x03'


def test_a_balance_argument_is_eight_bytes_not_sixteen():
    # The vault runs Balance = u64 because subtensor's chain Balance is rao-as-u64 (the Spike 0
    # finding). A u128 here would trap decoding on every call that carries an amount.
    assert len(codec.u64(10**9)) == 8


def test_fee_entries_encode_as_a_scale_vec_of_pairs():
    raw = codec.fee_entries([(ALICE, 7), (BOB, 9)])
    assert raw[0] == 0x08  # compact(2)
    assert raw[1:33] == bytes([1] * 32)
    assert raw[33:41] == (7).to_bytes(8, 'little')
    assert raw[41:73] == bytes([2] * 32)
    assert raw[73:81] == (9).to_bytes(8, 'little')
    assert len(raw) == 81


def test_an_empty_batch_still_encodes_and_the_contract_is_left_to_refuse_it():
    # vote_collect_fees_batch rejects an empty vector on-chain (InvalidBatch); the codec's job is
    # to encode faithfully, not to duplicate the contract's validation.
    assert codec.fee_entries([]) == b'\x00'


def test_a_swap_key_becomes_a_swap_ref_from_bytes_or_hex():
    raw = bytes(range(32))
    assert codec.hash32(raw) == raw
    assert codec.hash32(raw.hex()) == raw
    assert codec.hash32('0x' + raw.hex()) == raw
    with pytest.raises(codec.VaultCodecError):
        codec.hash32(bytes(31))


def test_an_account_round_trips_through_ss58():
    assert codec.account_bytes(codec.account_ss58(bytes([9] * 32))) == bytes([9] * 32)


# --- metadata -----------------------------------------------------------------------------------


def test_every_message_the_relayer_calls_exists_in_the_artifact(meta):
    for label in (
        'vote_slash',
        'vote_unlock',
        'vote_collect_fees_batch',
        'get_collateral',
        'get_lock_state',
        'get_settled_total',
        'is_slashed',
    ):
        assert len(meta.selector(label)) == 4


def test_an_unknown_message_names_itself_rather_than_encoding_garbage(meta):
    with pytest.raises(codec.VaultCodecError, match='vote_teleport'):
        meta.call('vote_teleport')


def test_a_vault_event_decodes_by_its_signature_topic(meta):
    spec = meta.events['MinerSlashed']
    data = bytes([3] * 32) + bytes([4] * 32) + _u128(50) + _u128(40) + _u128(10)
    assert meta.spec_for_topic(spec.signature_topic) is spec
    fields = meta.decode_event(spec, data)
    assert fields['miner'] == codec.account_ss58(bytes([3] * 32))
    assert fields['swap_ref'] == bytes([4] * 32)
    # Event amounts are u128 even though the call-side Balance is u64 — a stable indexer ABI.
    assert (fields['seized'], fields['reimbursed'], fields['surplus']) == (50, 40, 10)


def test_an_unrecognised_topic_is_not_guessed_at(meta):
    assert meta.spec_for_topic('0x' + 'ff' * 32) is None


class _EventSub:
    """Substrate stand-in shaped like a real node's event record: the signature topic sits on the
    RECORD, beside the event, and never inside the event's own attributes."""

    def __init__(self, records):
        self.records = records
        self.substrate = self

    def get_block_hash(self, block):
        return f'0xblock{block}'

    def get_events(self, _block_hash):
        return self.records


def _emitted(topic, data, contract=ALICE):
    return {
        'module_id': 'Contracts',
        'event_id': 'ContractEmitted',
        'attributes': {'contract': contract, 'data': data},
        'topics': [topic],
    }


def test_a_vault_event_is_found_by_the_topic_on_its_record(meta):
    spec = meta.events['CollateralPosted']
    data = bytes([3] * 32) + _u128(50) + _u128(70)
    sub = _EventSub([_emitted(spec.signature_topic, '0x' + data.hex())])
    events = BondVaultClient(sub, ALICE, metadata=meta).poll_events(1, 1)
    assert [e.name for e in events] == ['CollateralPosted']
    assert events[0].fields['total'] == 70


def test_an_event_payload_decodes_in_either_node_shape(meta):
    # `data` is a SCALE Bytes like every other blob, so a payload that happens to be valid UTF-8
    # arrives already decoded rather than as hex — same hazard as the read path.
    spec = meta.events['FeesRecycled']
    data = b'\x00' * 16
    sub = _EventSub([_emitted(spec.signature_topic, _as_node_shape(data))])
    events = BondVaultClient(sub, ALICE, metadata=meta).poll_events(1, 1)
    assert [(e.name, e.fields['tao_amount']) for e in events] == [('FeesRecycled', 0)]


def test_another_contracts_event_at_the_same_address_is_ignored(meta):
    sub = _EventSub([{'module_id': 'Contracts', 'event_id': 'Called', 'attributes': {'contract': ALICE}, 'topics': []}])
    assert BondVaultClient(sub, ALICE, metadata=meta).poll_events(1, 1) == []


# --- reads --------------------------------------------------------------------------------------


class _Sub:
    """Substrate stand-in: `runtime_call` returns whatever payload the test parks on it.

    Payloads come back in the SAME two shapes a real node produces — hex string for a blob that
    isn't valid UTF-8, the decoded characters for one that is (see `_return_bytes`)."""

    def __init__(self, payload=None, raise_on_read=False):
        self.payload = payload
        self.raise_on_read = raise_on_read
        self.substrate = self  # the client reaches through subtensor.substrate

    def runtime_call(self, *_a, **_kw):
        if self.raise_on_read:
            raise RuntimeError('ContractResult decode failed')
        return {'result': {'Ok': {'data': _as_node_shape(self.payload)}}}


def _as_node_shape(payload: bytes) -> str:
    try:
        return payload.decode('utf-8')
    except UnicodeDecodeError:
        return '0x' + payload.hex()


def _client(meta, payload=None, raise_on_read=False):
    return BondVaultClient(_Sub(payload, raise_on_read), ALICE, metadata=meta)


def test_lock_state_decodes_the_locked_flag_and_the_epoch(meta):
    c = _client(meta, b'\x00' + b'\x01' + (7).to_bytes(8, 'little'))  # Ok(()) prefix + (true, 7)
    assert c.get_lock_state(BOB) == (True, 7)


def test_a_read_decodes_whichever_shape_the_node_returns_it_in(meta):
    # The node hands a SCALE `Bytes` back as hex only when it isn't valid UTF-8; when it is, the
    # characters arrive decoded. Which shape a query lands in therefore depends on its own VALUE,
    # and the readings that matter most sit in the second one: a freshly locked bond (epoch 1), a
    # zero balance, and both arms of `is_slashed`. Reading those as "unknown" leaves the relayer
    # unable to attest, slash or unlock anything against a real vault.
    assert _as_node_shape(b'\x00\x01' + (1).to_bytes(8, 'little')) == '\x00\x01\x01' + '\x00' * 7
    assert _client(meta, b'\x00\x01' + (1).to_bytes(8, 'little')).get_lock_state(BOB) == (True, 1)
    assert _client(meta, b'\x00' + bytes(8)).get_collateral(BOB) == 0
    assert _client(meta, b'\x00' + bytes(8)).get_settled_total(BOB) == 0
    assert _client(meta, b'\x00\x01').is_slashed(bytes(32)) is True
    assert _client(meta, b'\x00\x00').is_slashed(bytes(32)) is False
    # ...and the hex shape still decodes, so the fix is additive.
    big = (10_000_000).to_bytes(8, 'little')
    assert _as_node_shape(b'\x00' + big).startswith('0x')
    assert _client(meta, b'\x00' + big).get_collateral(BOB) == 10_000_000


def test_an_undecodable_read_is_unknown_not_zero(meta):
    # A node that can't decode a dry-run must never be allowed to attest a bond away, so every
    # read answers None and every caller treats None as "leave it alone".
    c = _client(meta, raise_on_read=True)
    assert c.get_collateral(BOB) is None
    assert c.get_lock_state(BOB) is None
    assert c.get_settled_total(BOB) is None
    assert c.is_slashed(bytes(32)) is None


def test_a_lang_error_return_is_treated_as_unreadable(meta):
    c = _client(meta, b'\x01\x00')  # Err(LangError)
    assert c.get_collateral(BOB) is None


def test_a_truncated_payload_does_not_half_decode_into_a_number(meta):
    c = _client(meta, b'\x00\x01\x02')  # Ok + 2 bytes where a u64 belongs
    assert c.get_collateral(BOB) is None


def test_is_slashed_reads_the_permanent_marker(meta):
    assert _client(meta, b'\x00\x01').is_slashed(bytes(32)) is True
    assert _client(meta, b'\x00\x00').is_slashed(bytes(32)) is False


def test_a_pending_slash_option_decodes_both_arms(meta):
    assert _client(meta, b'\x00\x00').get_pending_slash(bytes(32)) is None
    payload = b'\x00\x01' + bytes([5] * 32) + (11).to_bytes(8, 'little')
    assert _client(meta, payload).get_pending_slash(bytes(32)) == (codec.account_ss58(bytes([5] * 32)), 11)


# --- call classification ------------------------------------------------------------------------


def _receipt(events, is_success=True, error=None):
    return SimpleNamespace(
        triggered_events=[{'event': {'module_id': m, 'event_id': e}} for m, e in events],
        is_success=is_success,
        error_message=error,
        extrinsic_hash='0xdead',
    )


def test_a_contract_err_surfaces_as_a_bare_contract_revert():
    # pallet-contracts flattens every ink! Err into one ContractReverted, which is exactly why the
    # slash relay re-reads `is_slashed` instead of trusting an error name.
    result = BondVaultClient._classify(
        _receipt([('System', 'ExtrinsicFailed')], is_success=False, error={'name': 'ContractReverted'})
    )
    assert not result.ok and result.reverted


def test_a_successful_call_carries_its_events_for_the_caller_to_show():
    result = BondVaultClient._classify(_receipt([('Contracts', 'ContractEmitted'), ('System', 'ExtrinsicSuccess')]))
    assert result.ok and result.error is None
    assert result.events == ['Contracts.ContractEmitted', 'System.ExtrinsicSuccess']


# --- configuration ------------------------------------------------------------------------------


def test_an_unconfigured_vault_says_how_to_configure_it(monkeypatch):
    monkeypatch.delenv('ALLWAYS_VAULT_ADDRESS', raising=False)
    with pytest.raises(VaultConfigError, match='vault-address'):
        resolve_vault_address({})


def test_env_beats_the_config_file_for_both_address_and_metadata(monkeypatch):
    monkeypatch.setenv('ALLWAYS_VAULT_ADDRESS', ALICE)
    monkeypatch.setenv('ALLWAYS_VAULT_METADATA', '/tmp/from-env.json')
    assert resolve_vault_address({'vault-address': BOB}) == ALICE
    assert resolve_metadata_path({'vault-metadata': '/tmp/from-file.json'}) == '/tmp/from-env.json'


def test_the_config_file_is_honoured_when_the_env_is_silent(monkeypatch):
    monkeypatch.delenv('ALLWAYS_VAULT_ADDRESS', raising=False)
    monkeypatch.delenv('ALLWAYS_VAULT_METADATA', raising=False)
    assert resolve_vault_address({'vault-address': BOB}) == BOB
    assert resolve_metadata_path({'vault-metadata': '/tmp/x.json'}) == '/tmp/x.json'


def _u128(n: int) -> bytes:
    return int(n).to_bytes(16, 'little')

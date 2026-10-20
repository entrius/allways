"""The EVM bind message and signature, pinned to the same vector as the program's
test_evm_bind_message_matches_the_shared_vector (smart-contracts/.../tests/test_base_hub.rs)."""

from eth_account import Account
from eth_account.messages import encode_defunct
from solders.pubkey import Pubkey

from allways.solana.client import evm_bind_message, sign_evm_bind

# anvil's account #0; the Solana pubkey is bytes 1..32.
EVM_KEY = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'
SOLANA = Pubkey.from_bytes(bytes(range(1, 33)))
PROGRAM_ID = Pubkey.from_string('6JVBEj5w27J2SVjERmv2c7wXgFee9nSSBKUJevHehyBD')

MESSAGE = (
    'allways: bind EVM address 0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266 to Solana '
    '4wBqpZM9xaSheZzJSMawUKKwhdpChKbZ5eu5ky4Vigw (program 6JVBEj5w27J2SVjERmv2c7wXgFee9nSSBKUJevHehyBD)'
)
DIGEST = '993a92a4d3f9a8d6c77e4f75460823c8b8e3cfa1650423c762129b9f186bf35e'
SIGNATURE = (
    'c512197956bd508d3ba52b4a8788d1afff0f71da8db904503be60fc0d714adba'
    '0357f8ae382560de32fbc704ac979be666c2cd93e1639875d9f98285d1b81eda1b'
)


def test_the_message_matches_the_program_vector():
    account = Account.from_key(EVM_KEY)
    assert evm_bind_message(bytes.fromhex(account.address[2:]), SOLANA, PROGRAM_ID) == MESSAGE


def test_the_eip191_digest_matches_the_program_vector():
    assert Account.from_key(EVM_KEY).sign_message(encode_defunct(text=MESSAGE)).message_hash.hex() == DIGEST


def test_the_signature_matches_the_program_vector_byte_for_byte():
    sig = sign_evm_bind(Account.from_key(EVM_KEY), SOLANA, PROGRAM_ID)
    assert sig.hex() == SIGNATURE
    assert len(sig) == 65 and sig[64] in (27, 28)


def test_the_signature_names_the_solana_pubkey():
    account = Account.from_key(EVM_KEY)
    other = Pubkey.from_bytes(bytes(range(2, 34)))
    assert sign_evm_bind(account, other, PROGRAM_ID) != sign_evm_bind(account, SOLANA, PROGRAM_ID)

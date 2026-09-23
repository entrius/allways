"""redact_rpc_url masks provider API keys however they ride the URL — query param
(Helius) or bare path segment (Alchemy) — while leaving keyless endpoints readable.
"""

from allways.solana.rpc import redact_rpc_url


def test_query_param_key_is_masked():
    url = 'https://mainnet.helius-rpc.com/?api-key=abcd1234-ef56-7890-abcd-1234567890ab'
    assert redact_rpc_url(url) == 'https://mainnet.helius-rpc.com/?api-key=***'


def test_path_segment_key_is_masked():
    url = 'https://eth-mainnet.g.alchemy.com/v2/AbCdEfGhIjKlMnOpQrStUvWxYz123456'
    assert redact_rpc_url(url) == 'https://eth-mainnet.g.alchemy.com/v2/***'


def test_keyless_url_unchanged():
    assert redact_rpc_url('http://127.0.0.1:8899') == 'http://127.0.0.1:8899'
    assert redact_rpc_url('https://api.devnet.solana.com') == 'https://api.devnet.solana.com'


def test_every_query_value_is_masked():
    out = redact_rpc_url('https://rpc.example.com/?api-key=secret&session=alsosecret')
    assert 'secret' not in out
    assert out == 'https://rpc.example.com/?api-key=***&session=***'

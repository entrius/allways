"""Canonical proof-message formats signed by users and verified by validators.

Kept in one place so the CLI signer and validator verifier cannot drift — a
typo on either side would silently invalidate every proof.
"""


def swap_proof_message(from_tx_hash: str) -> str:
    return f'allways-swap:{from_tx_hash}'

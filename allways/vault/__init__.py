"""TAO bond vault (Bittensor side of the v2 split-collateral design).

``client.BondVaultClient`` is the one place that speaks to the ``allways_bond_vault`` ink!
contract: the miner/admin surface the ``alw vault`` CLI drives, and the quorum relay rounds
the validator's W3 relayer drives. ``codec`` holds the SCALE encode/decode primitives and the
metadata reader (selectors + event signature topics) both sides share.
"""

from allways.vault.client import (
    BondVaultClient,
    VaultCallResult,
    VaultConfigError,
    VaultEvent,
    resolve_vault_address,
)

__all__ = [
    'BondVaultClient',
    'VaultCallResult',
    'VaultConfigError',
    'VaultEvent',
    'resolve_vault_address',
]

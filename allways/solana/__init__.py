"""Solana client for the allways_swap_manager program (B0 foundation).

Hand-rolled + sync: `solders` (keys/tx primitives) + `borsh-construct` (account/arg layouts) over a
thin JSON-RPC layer. Replaces the ink!/Substrate `allways.contract_client`. See
smart-contracts/SOLANA_VALIDATOR_CHANGES.md (★ Validator rewrite roadmap → B0).
"""

from allways.solana.pdas import PROGRAM_ID

__all__ = ['PROGRAM_ID']

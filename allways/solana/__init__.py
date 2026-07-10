"""Solana client for the allways_swap_manager program (B0 foundation).

Hand-rolled + sync: `solders` (keys/tx primitives) + `borsh-construct` (account/arg layouts) over a
thin JSON-RPC layer. The validator's sole on-chain client (the old ink!/Substrate client is gone).
"""

from allways.solana.program import resolve_program_id

__all__ = ['resolve_program_id']

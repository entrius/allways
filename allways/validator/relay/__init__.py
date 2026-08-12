"""W3 — the cross-chain bond relayer.

Solana is the transaction ledger; the Bittensor vault is the collateral. Neither can read the
other, so every cross-chain action is a verdict quorum'd on one chain and carried by validators
to a second quorum on the other. This package is the validator half of that carry, in three jobs:

* **slash relay** (Solana → vault) — a timeout verdict becomes ``vote_slash``, netted into the
  miner's attestation FIRST so the mirror leads the vault in the pessimistic direction
  (:mod:`slash`).
* **attestation maintenance** (vault → Solana) — the effective bond Solana's guards read:
  vault gross − unsettled fees − unapplied slash verdicts. Event-driven only (:mod:`attestation`).
* **exit + fee settlement** (Solana → vault) — the quiescence-gated residual settle and
  ``vote_unlock``, plus the time-aligned global fee true-up (:mod:`exit_relay`).

:class:`BondRelay` (:mod:`engine`) owns the three, the lazy global heartbeat, and the
reconcile-before-heartbeat restart barrier.
"""

from allways.validator.relay.engine import BondRelay, RelayConfig

__all__ = ['BondRelay', 'RelayConfig']

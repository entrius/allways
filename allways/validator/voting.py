"""Handles validator voting on swap outcomes."""

import bittensor as bt

from allways.contract_client import AllwaysContractClient


class SwapVoter:
    """Confirms or times out swaps via the smart contract."""

    def __init__(self, contract_client: AllwaysContractClient, wallet: bt.Wallet):
        self.client = contract_client
        self.wallet = wallet

    def confirm_swap(self, swap_id: int) -> bool:
        """Confirm a fulfilled swap (quorum mechanism)."""
        bt.logging.info(f'Confirming swap {swap_id}')
        try:
            self.client.confirm_swap(wallet=self.wallet, swap_id=swap_id)
            return True
        except Exception as e:
            bt.logging.error(f'Confirm swap failed for swap {swap_id}: {e}')
            return False

    def timeout_swap(self, swap_id: int) -> bool:
        """Timeout a swap (single trigger)."""
        bt.logging.info(f'Timing out swap {swap_id}')
        try:
            self.client.timeout_swap(wallet=self.wallet, swap_id=swap_id)
            return True
        except Exception as e:
            bt.logging.error(f'Timeout swap failed for swap {swap_id}: {e}')
            return False

    def extend_timeout(self, swap_id: int) -> bool:
        """Extend a fulfilled swap's timeout (dest tx needs more confirmations).

        Raises on failure so the caller can distinguish expected rejections
        (AlreadyVoted, ContractReverted) from real errors.
        """
        self.client.vote_extend_timeout(wallet=self.wallet, swap_id=swap_id)
        return True

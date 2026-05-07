import copy
import time
from abc import ABC, abstractmethod
from typing import Callable

import bittensor as bt
from websockets.exceptions import ConnectionClosedError

from allways import __spec_version__ as spec_version
from allways.constants import STALE_BLOCK_POLL_THRESHOLD
from allways.utils.config import add_args, check_config, config
from allways.utils.misc import ttl_get_block


class BaseNeuron(ABC):
    """
    Base class for Bittensor neurons. This class is abstract and should be inherited by a subclass.
    It contains the core logic for all neurons; validators and miners.
    """

    neuron_type: str = 'BaseNeuron'

    @classmethod
    def check_config(cls, config: 'bt.Config'):
        check_config(cls, config)

    @classmethod
    def add_args(cls, parser):
        add_args(cls, parser)

    @classmethod
    def config(cls):
        return config(cls)

    subtensor: 'bt.subtensor'
    wallet: 'bt.wallet'
    metagraph: 'bt.metagraph'
    spec_version: int = spec_version

    @property
    def block(self):
        return ttl_get_block(self)

    def __init__(self, config=None):
        base_config = copy.deepcopy(config or BaseNeuron.config())
        self.config = self.config()
        self.config.merge(base_config)
        self.check_config(self.config)

        bt.logging.set_config(config=self.config.logging)

        self.device = self.config.neuron.device

        bt.logging.info('Setting up bittensor objects.')

        self.wallet = bt.Wallet(config=self.config)
        self.subtensor = bt.Subtensor(config=self.config)
        self.metagraph = self.subtensor.metagraph(self.config.netuid)

        bt.logging.info(f'Wallet: {self.wallet}')
        bt.logging.info(f'Subtensor: {self.subtensor}')
        bt.logging.info(f'Metagraph: {self.metagraph}')

        self.check_registered()

        self.uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
        bt.logging.info(
            f'Running neuron on subnet: {self.config.netuid} with uid {self.uid} '
            f'using network: {self.subtensor.chain_endpoint}'
        )
        self.step = 0
        self.last_seen_block = 0
        self.stale_block_polls = 0

    def check_block_progress(self, reconnect: Callable[[], None]) -> None:
        """Reconnect if the substrate WS appears wedged — chain head frozen across many polls."""
        try:
            current_block = self.subtensor.get_current_block()
        except Exception as e:
            bt.logging.debug(f'block-progress watchdog: get_current_block failed: {e}')
            return

        if current_block == self.last_seen_block:
            self.stale_block_polls += 1
        else:
            self.stale_block_polls = 0
            self.last_seen_block = current_block

        if self.stale_block_polls >= STALE_BLOCK_POLL_THRESHOLD:
            bt.logging.warning(
                f'chain head frozen at {current_block} for {self.stale_block_polls} polls, reconnecting subtensor'
            )
            reconnect()
            self.stale_block_polls = 0

    def reconnect_subtensor(self):
        """Recreate subtensor connection when WebSocket goes stale."""
        bt.logging.info('Reconnecting subtensor...')
        old_subtensor = self.subtensor
        self.subtensor = bt.Subtensor(config=self.config)
        try:
            old_subtensor.close()
        except Exception:
            pass

    @abstractmethod
    async def forward(self) -> None: ...

    @abstractmethod
    def run(self) -> None: ...

    def sync(self):
        """Wrapper for synchronizing the state of the network for the given miner or validator."""
        # Registration only changes at epoch boundaries; cold-start covered by __init__.
        if self.should_sync_metagraph():
            self.check_registered()
            self.resync_metagraph()

        if self.should_set_weights():
            self.set_weights()

        self.save_state()

    def check_registered(self, max_retries: int = 3):
        """Check if hotkey is registered, with retry logic for connection failures."""
        for attempt in range(max_retries):
            try:
                if not self.subtensor.is_hotkey_registered(
                    netuid=self.config.netuid,
                    hotkey_ss58=self.wallet.hotkey.ss58_address,
                ):
                    bt.logging.error(
                        f'Wallet: {self.wallet} is not registered on netuid {self.config.netuid}.'
                        f' Please register the hotkey using `btcli subnets register` before trying again'
                    )
                    raise SystemExit(1)
                return
            except ConnectionClosedError as e:
                bt.logging.warning(
                    f'WebSocket connection closed during check_registered (attempt {attempt + 1}/{max_retries}): {e}'
                )
                if attempt < max_retries - 1:
                    self.reconnect_subtensor()
                    time.sleep(2**attempt)
                else:
                    raise

    def should_sync_metagraph(self):
        """Check if enough epoch blocks have elapsed since the last checkpoint to sync."""
        return (self.block - self.metagraph.last_update[self.uid]) > self.config.neuron.epoch_length

    def should_set_weights(self) -> bool:
        if self.step == 0:
            return False

        if self.config.neuron.disable_set_weights:
            return False

        return (
            self.block - self.metagraph.last_update[self.uid]
        ) > self.config.neuron.epoch_length and self.neuron_type != 'MinerNeuron'

    def save_state(self):
        bt.logging.trace(
            'save_state() not implemented for this neuron. '
            'You can implement this function to save model checkpoints or other useful data.'
        )

    def load_state(self):
        bt.logging.trace(
            'load_state() not implemented for this neuron. '
            'You can implement this function to load model checkpoints or other useful data.'
        )

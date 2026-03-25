import argparse
import asyncio
import threading
import time
from typing import Union

import bittensor as bt

from allways.utils.config import add_miner_args
from neurons.base.neuron import BaseNeuron


class BaseMinerNeuron(BaseNeuron):
    """
    Base class for Bittensor miners. Polling-based (no axon).
    """

    neuron_type: str = 'MinerNeuron'

    @classmethod
    def add_args(cls, parser: argparse.ArgumentParser):
        super().add_args(parser)
        add_miner_args(cls, parser)

    def __init__(self, config=None):
        super().__init__(config=config)

        self.loop = asyncio.get_event_loop()

        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: Union[threading.Thread, None] = None

    def run(self):
        """
        Main loop for the miner. Polls for new swaps and syncs with the network.
        """
        self.sync()

        bt.logging.info(f'Miner starting at block: {self.block}')

        consecutive_errors = 0

        try:
            while not self.should_exit:
                try:
                    self.loop.run_until_complete(self.forward())

                    if self.should_sync_metagraph():
                        self.sync()

                    consecutive_errors = 0
                except KeyboardInterrupt:
                    raise
                except Exception as step_err:
                    consecutive_errors += 1
                    bt.logging.error(f'Step {self.step} error ({consecutive_errors} consecutive): {step_err}')
                    self._reconnect_subtensor()
                    time.sleep(min(2**consecutive_errors, 30))

                self.step += 1
                time.sleep(self.config.miner.poll_interval)

        except KeyboardInterrupt:
            bt.logging.success('Miner killed by keyboard interrupt.')
            self.should_exit = True

    def run_in_background_thread(self):
        """Starts the miner's operations in a separate background thread."""
        if not self.is_running:
            bt.logging.debug('Starting miner in background thread.')
            self.should_exit = False
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            self.is_running = True
            bt.logging.debug('Started')

    def stop_run_thread(self):
        """Stops the miner's operations that are running in the background thread."""
        if self.is_running:
            bt.logging.debug('Stopping miner in background thread.')
            self.should_exit = True
            if self.thread is not None:
                self.thread.join(5)
            self.is_running = False
            bt.logging.debug('Stopped')

    def __enter__(self):
        self.run_in_background_thread()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_run_thread()

    def resync_metagraph(self):
        """Resyncs the metagraph and updates the hotkeys and moving averages based on the new metagraph."""
        bt.logging.debug('resync_metagraph()')
        self.metagraph.sync(subtensor=self.subtensor)

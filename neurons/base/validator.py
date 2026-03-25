import argparse
import asyncio
import copy
import threading
import time
from typing import List, Set, Union

import bittensor as bt
import numpy as np

from allways.constants import VALIDATOR_POLL_INTERVAL_SECONDS
from allways.utils.config import add_validator_args
from neurons.base.neuron import BaseNeuron
from neurons.base.utils.weight_utils import (
    convert_weights_and_uids_for_emit,
    process_weights_for_netuid,
)


class BaseValidatorNeuron(BaseNeuron):
    """
    Base class for Bittensor validators. Your validator should inherit from this class.
    """

    neuron_type: str = 'ValidatorNeuron'

    @classmethod
    def add_args(cls, parser: argparse.ArgumentParser):
        super().add_args(parser)
        add_validator_args(cls, parser)

    def __init__(self, config=None):
        super().__init__(config=config)

        # Save a copy of the hotkeys to local memory.
        self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)

        # Set up initial scoring weights for validation
        bt.logging.info('Building validation weights.')
        self.scores = np.zeros(self.metagraph.n, dtype=np.float32)

        # Restore persisted scores before sync (which calls save_state)
        self.load_state()

        # Init sync with the network. Updates the metagraph.
        self.sync()

        # Serve axon to enable external connections.
        if not self.config.neuron.axon_off:
            self.serve_axon()
        else:
            bt.logging.warning('axon off, not serving ip to chain.')

        # Create asyncio event loop to manage async tasks.
        self.loop = asyncio.get_event_loop()

        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: Union[threading.Thread, None] = None

    def serve_axon(self):
        """Serve axon to enable external connections."""
        bt.logging.info('serving ip to chain...')
        try:
            self.axon = bt.Axon(wallet=self.wallet, config=self.config)

            for attempt in range(3):
                try:
                    self.subtensor.serve_axon(netuid=self.config.netuid, axon=self.axon)
                    metagraph = self.subtensor.metagraph(self.config.netuid)
                    uid = metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
                    if metagraph.axons[uid].is_serving:
                        break
                    bt.logging.warning(f'Axon serve attempt {attempt + 1}: not yet serving on chain, retrying...')
                    time.sleep(5)
                except Exception as e:
                    bt.logging.error(f'Failed to serve Axon (attempt {attempt + 1}): {e}')
                    if attempt < 2:
                        time.sleep(5)

            self.axon.start()
            bt.logging.info(
                f'Running validator {self.axon} on network: '
                f'{self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}'
            )

        except Exception as e:
            bt.logging.error(f'Failed to initialize Axon: {e}')

    async def concurrent_forward(self):
        await self.forward()

    def run(self):
        """
        Main loop for the validator. Runs forward pass, syncs with network.
        """
        self.sync()

        bt.logging.info(f'Validator starting at block: {self.block}')

        consecutive_errors = 0

        try:
            while True:
                try:
                    bt.logging.info(f'step({self.step}) block({self.block})')
                    self.loop.run_until_complete(self.concurrent_forward())
                    self.sync()
                    consecutive_errors = 0
                except KeyboardInterrupt:
                    raise
                except Exception as step_err:
                    consecutive_errors += 1
                    bt.logging.error(f'Step {self.step} error ({consecutive_errors} consecutive): {step_err}')
                    self._reconnect_subtensor()
                    time.sleep(min(2**consecutive_errors, 30))

                if self.should_exit:
                    break

                self.step += 1

                poll_interval = getattr(
                    getattr(self.config, 'validator', None),
                    'poll_interval',
                    VALIDATOR_POLL_INTERVAL_SECONDS,
                )
                time.sleep(poll_interval)

        except KeyboardInterrupt:
            if hasattr(self, 'axon'):
                self.axon.stop()
            bt.logging.success('Validator killed by keyboard interrupt.')
            self.should_exit = True

    def run_in_background_thread(self):
        """Starts the validator's operations in a background thread."""
        if not self.is_running:
            bt.logging.debug('Starting validator in background thread.')
            self.should_exit = False
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            self.is_running = True
            bt.logging.debug('Started')

    def stop_run_thread(self):
        """Stops the validator's operations that are running in the background thread."""
        if self.is_running:
            bt.logging.debug('Stopping validator in background thread.')
            self.should_exit = True
            self.thread.join(5)
            self.is_running = False
            bt.logging.debug('Stopped')

    def __enter__(self):
        self.run_in_background_thread()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, 'axon'):
            self.axon.stop()
        self.stop_run_thread()

    def set_weights(self):
        """Sets the validator weights to the metagraph hotkeys based on the scores it has received from the miners."""
        if np.isnan(self.scores).any():
            bt.logging.warning(
                'Scores contain NaN values. This may be due to a lack of responses from miners, '
                'or a bug in your reward functions.'
            )

        norm = np.linalg.norm(self.scores, ord=1, axis=0, keepdims=True)

        if np.any(norm == 0) or np.isnan(norm).any():
            norm = np.ones_like(norm)

        raw_weights = self.scores / norm

        bt.logging.debug('raw_weights', raw_weights)
        bt.logging.debug('raw_weight_uids', str(self.metagraph.uids.tolist()))
        (
            processed_weight_uids,
            processed_weights,
        ) = process_weights_for_netuid(
            uids=self.metagraph.uids,
            weights=raw_weights,
            netuid=self.config.netuid,
            subtensor=self.subtensor,
            metagraph=self.metagraph,
        )
        bt.logging.debug('processed_weights', processed_weights)
        bt.logging.debug('processed_weight_uids', processed_weight_uids)

        (
            uint_uids,
            uint_weights,
        ) = convert_weights_and_uids_for_emit(uids=processed_weight_uids, weights=processed_weights)
        bt.logging.debug('uint_weights', uint_weights)
        bt.logging.debug('uint_uids', uint_uids)

        result, msg = self.subtensor.set_weights(
            wallet=self.wallet,
            netuid=self.config.netuid,
            uids=uint_uids,
            weights=uint_weights,
            wait_for_finalization=False,
            wait_for_inclusion=False,
            version_key=self.spec_version,
        )
        if result is True:
            bt.logging.info('set_weights on chain successfully!')
        else:
            bt.logging.error('set_weights failed', msg)

    def resync_metagraph(self):
        """Resyncs the metagraph and updates the hotkeys and moving averages based on the new metagraph."""
        bt.logging.info('resync_metagraph()')

        previous_metagraph = copy.deepcopy(self.metagraph)

        self.metagraph.sync(subtensor=self.subtensor)

        if previous_metagraph.axons == self.metagraph.axons:
            return

        bt.logging.info('Metagraph updated, re-syncing hotkeys and moving averages')
        for uid, hotkey in enumerate(self.hotkeys):
            if hotkey != self.metagraph.hotkeys[uid]:
                self.scores[uid] = 0

        if len(self.hotkeys) < len(self.metagraph.hotkeys):
            new_moving_average = np.zeros((self.metagraph.n))
            min_len = min(len(self.hotkeys), len(self.scores))
            new_moving_average[:min_len] = self.scores[:min_len]
            self.scores = new_moving_average

        self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)

    def update_scores(self, rewards: np.ndarray, uids: Set[int], blacklisted_uids: List[int] = None):
        """Performs exponential moving average on the scores based on the rewards received from the miners."""
        if np.isnan(rewards).any():
            bt.logging.warning(f'NaN values detected in rewards: {rewards}')
            rewards = np.nan_to_num(rewards, nan=0)

        rewards = np.asarray(rewards)

        if isinstance(uids, np.ndarray):
            uids_array = uids.copy()
        else:
            uids_array = np.array(sorted(list(uids)))

        if rewards.size == 0 or uids_array.size == 0:
            bt.logging.info(f'rewards: {rewards}, uids_array: {uids_array}')
            bt.logging.warning('Either rewards or uids_array is empty. No updates will be performed.')
            return

        if rewards.size != uids_array.size:
            raise ValueError(
                f'Shape mismatch: rewards array of shape {rewards.shape} '
                f'cannot be broadcast to uids array of shape {uids_array.shape}'
            )

        scattered_rewards: np.ndarray = np.zeros_like(self.scores)
        scattered_rewards[uids_array] = rewards
        bt.logging.debug(f'Scattered rewards: {rewards}')

        alpha: float = self.config.neuron.moving_average_alpha
        self.scores: np.ndarray = alpha * scattered_rewards + (1 - alpha) * self.scores
        bt.logging.debug(f'Updated moving avg scores: {self.scores}')

        if blacklisted_uids:
            blacklisted_uids_array = np.array(blacklisted_uids)
            self.scores[blacklisted_uids_array] = 0.0
            bt.logging.info(f'Set scores to 0 for blacklisted UIDs: {blacklisted_uids}')

            total_score = np.sum(self.scores)
            if total_score > 0:
                self.scores = self.scores / total_score
                bt.logging.debug(f'Renormalized scores to sum=1 after blacklisting. New sum: {np.sum(self.scores)}')

    def save_state(self):
        """Saves the state of the validator to a file."""
        bt.logging.info('Saving validator state.')

        np.savez(
            self.config.neuron.full_path + '/state.npz',
            step=self.step,
            scores=self.scores,
            hotkeys=self.hotkeys,
        )

    def load_state(self):
        """Loads the state of the validator from a file."""
        bt.logging.info('Loading validator state.')

        state_path = self.config.neuron.full_path + '/state.npz'
        try:
            state = np.load(state_path)
            self.step = int(state['step'])
            self.scores = state['scores']
            self.hotkeys = list(state['hotkeys'])
            bt.logging.success(f'Successfully loaded validator state from {state_path}')
        except FileNotFoundError:
            bt.logging.warning(f'No state file found at {state_path}, starting with fresh state')
        except Exception as e:
            bt.logging.error(f'Failed to load validator state from {state_path}: {e}. Starting with fresh state')

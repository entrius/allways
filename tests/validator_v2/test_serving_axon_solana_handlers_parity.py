"""Parity: tests/test_axon_solana_handlers.py with the v2 serving store in place of the old state store."""

from tests.validator_v2.parity import under_serving_store

globals().update(under_serving_store('test_axon_solana_handlers'))

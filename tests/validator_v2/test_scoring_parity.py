"""Parity: the scoring tests (tests/test_scoring_v1.py) run unchanged with the v2 ledger in place of the old
state store and event index (see parity.LedgerStore). The scoring math under test is the same function."""

from tests.validator_v2.parity import under_ledger

globals().update(under_ledger('test_scoring_v1'))

"""`post-tx` auto-retries a zero-accept relay only when the failure is non-deterministic.

A BTC deposit fired at `post-tx` immediately after broadcast hasn't propagated to validators'
bitcoind yet, so the first relay is rejected `tx_not_found` (deterministic=False) — a re-broadcast a
few seconds later succeeds. A genuine mismatch (wrong amount/recipient, expired reservation) is
deterministic=True and must fail fast, not spin."""

import types

from allways.cli.swap_commands.post_tx import _should_retry_relay


def _info(accepted, deterministic):
    return types.SimpleNamespace(accepted=accepted, deterministic=deterministic)


def test_retry_when_zero_accept_and_not_deterministic():
    assert _should_retry_relay(_info(0, False)) is True  # propagation lag / rate-limit / timeout


def test_no_retry_when_reject_is_deterministic():
    assert _should_retry_relay(_info(0, True)) is False  # wrong amount/recipient — resend can't help


def test_no_retry_once_a_validator_accepts():
    assert _should_retry_relay(_info(1, False)) is False  # quorum already progressing


def test_no_retry_on_missing_info():
    assert _should_retry_relay(None) is False

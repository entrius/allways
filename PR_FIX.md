## Prevent miner slashing when chain provider is unreachable

Right now `verify_transaction()` returns `None` for both "transaction not found" and "API is down." The validator treats both the same — assume the miner never sent funds, eventually timeout and slash.

This means a miner who fulfilled correctly can lose collateral just because Blockstream was temporarily unreachable.

### What changed

Added a `ProviderUnreachableError` exception that providers raise on transient failures (connection errors, timeouts, HTTP 5xx). The existing `Optional[TransactionInfo]` return stays for found/not-found.

Three call sites catch this exception:

- **Pending confirms** — keeps the item for retry instead of dropping it
- **Fulfilled verification** — marks the swap as uncertain so it isn't timed out
- **Timeout check** — skips swaps where verification was inconclusive this cycle

BTC RPC → Blockstream fallback is unchanged. If RPC says "not found", Blockstream is still tried.

### Before

```
API down → verify returns None → timeout → miner slashed
```

### After

```
API down → ProviderUnreachableError → defer timeout → retry next cycle
```

Unrelated method extractions (`_match_transfer`, `_rpc_resolve_sender`, `_blockstream_calc_confirmations`) have been moved to a separate PR (#18) to keep this diff focused on the fix.

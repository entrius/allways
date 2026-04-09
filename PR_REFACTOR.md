## Extract helper methods for readability across chain providers and axon handlers

Several long methods had too many responsibilities packed into one function, making them hard to follow. This PR pulls out focused helper methods without changing any behavior.

### What changed

**bitcoin.py**
- `_rpc_resolve_sender()` — extracts the sender address lookup from the first vin inside `_rpc_verify_transaction`
- `_blockstream_calc_confirmations()` — extracts the tip-height fetch and confirmation math from `_blockstream_verify_transaction`
- `_resolve_sender_utxos()` — extracts address matching and UTXO fetching from `send_amount_lightweight` (was ~45 lines deep)
- `_select_utxos()` — extracts greedy coin selection and fee calculation from `send_amount_lightweight`
- `_broadcast_tx()` — extracts the Blockstream broadcast call from `send_amount_lightweight`

**subtensor.py**
- `_match_transfer()` — extracts the extrinsic-matching logic from the inner loop of `verify_transaction`

**axon_handlers.py**
- `_load_swap_commitment()` — consolidates the duplicated commitment read + chain validation in `handle_swap_reserve` and `handle_swap_confirm`
- `_resolve_swap_direction()` — extracts deposit/fulfillment address resolution and rate lookup from `handle_swap_confirm`

### Why

`send_amount_lightweight` was ~177 lines handling address resolution, UTXO fetching, coin selection, tx building, signing, and broadcasting. Now it reads as a clear sequence of steps.

The commitment validation pattern was copy-pasted across two handlers. The direction resolution block was ~15 lines of address juggling that obscured the main flow.

No behavioral changes, no new dependencies, just reorganization.

#!/usr/bin/env bash
# Spike 0 — deploy the bond-vault scaffold to subtensor TESTNET and dust-call
# add_stake_recycle (chain ext 0x1000 fn 18) through the full slash→recycle path.
#
# Feasibility is already code-verified against finney v443 (see
# SOLANA_BITTENSOR_SPLIT_COLLATERAL.md "Kickoff checklist"); this validates the
# live mechanics: weight charge, storage deposits, Output decode, event flow.
#
# The deployer account plays every role (owner + sole validator + miner + user),
# so quorum = 1 vote and one wallet drives the whole flow:
#   build → instantiate → add_validator(self) → post_collateral(0.05τ)
#   → vote_slash(self, penalty 0.02τ, reimbursement 0)  [fills the fee pot]
#   → recycle_fees()                                    [THE dust-call]
#   → withdraw remaining collateral back.
# Total spend ≈ 0.02τ recycled + tx fees + refundable storage deposits.
#
# Required env:
#   SURI              secret for the deploying coldkey ("word word ..." mnemonic
#                     or //seed). Wallet "test" coldkey is encrypted on disk, so
#                     paste the mnemonic:  SURI="..." ./spike0.sh
# Optional env:
#   URL               ws endpoint      (default wss://test.finney.opentensor.ai:443)
#   NETUID            target subnet    (default 19 — allways testnet)
#   STAKING_HOTKEY    SS58 recycle target; MUST be registered on $NETUID
#                     (default: read from ~/.bittensor/wallets/test/hotkeys/test-test)
#   DEPLOYER_SS58     deployer address (default: ~/.bittensor/wallets/test/coldkeypub.txt)
#   CONTRACT          skip instantiate and reuse an existing deployment

set -euo pipefail
cd "$(dirname "$0")"

# System rust (1.97+) can't build-std for wasm (panic_immediate_abort removed);
# build through the rustup 1.89 toolchain instead. Override via RUSTUP_TOOLCHAIN.
export PATH="$HOME/.cargo/bin:$PATH"
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.89.0-x86_64-unknown-linux-gnu}"

# Localnet mode: LOCAL=1 runs against a local subtensor (same runtime code as
# testnet/finney, so the mechanics rehearsal is equivalent). One-time setup:
#   docker run --rm -d -p 9944:9944 --name subtensor-local \
#     ghcr.io/opentensor/subtensor-localnet:latest
#   btcli subnet create --network ws://127.0.0.1:9944 ...       # → netuid 2
#   btcli subnet start --netuid 2 ... ; btcli subnet register --netuid 2 ...
#   LOCAL=1 NETUID=2 STAKING_HOTKEY=<registered ss58> ./spike0.sh
if [ "${LOCAL:-0}" = "1" ]; then
  URL="${URL:-ws://127.0.0.1:9944}"
  SURI="${SURI:-//Alice}"
  DEPLOYER_SS58="${DEPLOYER_SS58:-5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY}"  # //Alice
  # The GHCR localnet image runs an older runtime whose ContractResult layout
  # cargo-contract 5.x can't decode in dry-runs; submit with explicit gas.
  SKIP_DRY_RUN="${SKIP_DRY_RUN:-1}"
fi
GAS_ARGS=""
[ "${SKIP_DRY_RUN:-0}" = "1" ] && GAS_ARGS="--skip-dry-run --gas 300000000000 --proof-size 2000000"

URL="${URL:-wss://test.finney.opentensor.ai:443}"
NETUID="${NETUID:-19}"
WALLET_DIR="$HOME/.bittensor/wallets/test"
OUT_DIR=".spike0"
mkdir -p "$OUT_DIR"

# Amounts in rao (1 τ = 1e9)
MIN_COLLATERAL=10000000    # 0.01 τ constructor floor
POST_AMOUNT=50000000       # 0.05 τ posted as bond
PENALTY=20000000           # 0.02 τ slashed with reimbursement 0 → all surplus → fee pot
VOTE_ROUND_TTL=600
SWAP_REF="0x0101010101010101010101010101010101010101010101010101010101010101"

command -v cargo-contract >/dev/null 2>&1 || command -v cargo contract >/dev/null 2>&1 || {
  echo "cargo-contract missing. Install: cargo install cargo-contract --locked" >&2; exit 1; }
[ -n "${SURI:-}" ] || { echo "Set SURI to the deploying coldkey mnemonic/seed." >&2; exit 1; }

if [ -z "${DEPLOYER_SS58:-}" ]; then
  DEPLOYER_SS58=$(python3 -c "import json;print(json.load(open('$WALLET_DIR/coldkeypub.txt'))['ss58Address'])")
fi
if [ -z "${STAKING_HOTKEY:-}" ]; then
  STAKING_HOTKEY=$(python3 -c "import json;print(json.load(open('$WALLET_DIR/hotkeys/test-test'))['ss58Address'])")
  echo "WARN: defaulting STAKING_HOTKEY=$STAKING_HOTKEY — verify it is registered on netuid $NETUID," >&2
  echo "      or the recycle step will revert (set STAKING_HOTKEY to any registered hotkey)." >&2
fi

echo "== deployer:       $DEPLOYER_SS58"
echo "== staking_hotkey: $STAKING_HOTKEY  (netuid $NETUID)"
echo "== endpoint:       $URL"

call() { # call <message> [--value N] [args...]
  local msg="$1"; shift
  # shellcheck disable=SC2086
  cargo contract call --contract "$CONTRACT" --message "$msg" --url "$URL" \
    --suri "$SURI" --execute --skip-confirm $GAS_ARGS "$@"
}
query() { # dry-run read (unavailable when dry-runs are broken on this runtime)
  local msg="$1"; shift
  if [ -n "$GAS_ARGS" ]; then echo "(query $msg skipped: dry-run undecodable on this runtime — verify via events above)"; return 0; fi
  cargo contract call --contract "$CONTRACT" --message "$msg" --url "$URL" \
    --suri "$SURI" "$@" 2>/dev/null | grep -A 2 "Result" || true
}

echo "== [1/7] cargo contract build --release"
cargo contract build --release

if [ -z "${CONTRACT:-}" ]; then
  echo "== [2/7] instantiate on testnet"
# shellcheck disable=SC2086
  cargo contract instantiate --url "$URL" --suri "$SURI" \
    --args "$STAKING_HOTKEY" "$NETUID" "$MIN_COLLATERAL" 0 100 "$VOTE_ROUND_TTL" \
    --execute --skip-confirm $GAS_ARGS --output-json | tee "$OUT_DIR/instantiate.json"
  CONTRACT=$(python3 -c "import json;print(json.load(open('$OUT_DIR/instantiate.json'))['contract'])")
  echo "$CONTRACT" > "$OUT_DIR/contract_address"
else
  echo "== [2/7] reusing CONTRACT=$CONTRACT"
fi
echo "== contract: $CONTRACT"

echo "== [3/7] add_validator(self)   # quorum becomes 1-of-1"
call add_validator --args "$DEPLOYER_SS58"

echo "== [4/7] post_collateral 0.05τ"
call post_collateral --value "$POST_AMOUNT"

echo "== [5/7] vote_slash(self, penalty 0.02τ, reimbursement 0) → fee pot"
call vote_slash --args "$DEPLOYER_SS58" "$SWAP_REF" "$PENALTY" "$DEPLOYER_SS58" 0
echo "-- accumulated_fees (expect $PENALTY):"
query get_accumulated_fees

echo "== [6/7] recycle_fees — the dust-call through ext 0x1000 fn 18"
call recycle_fees
echo "-- accumulated_fees (expect 0):"
query get_accumulated_fees
echo "-- total_recycled_fees (expect $PENALTY):"
query get_total_recycled_fees

echo "== [7/7] withdraw remaining bond (0.03τ) back to deployer"
call withdraw_collateral --args $((POST_AMOUNT - PENALTY))

echo ""
echo "SPIKE 0 PASSED — add_stake_recycle exercised live on testnet netuid $NETUID."
echo "Contract kept at: $CONTRACT ($OUT_DIR/contract_address). Record fee/weight"
echo "numbers from the outputs above into SOLANA_BITTENSOR_SPLIT_COLLATERAL.md."

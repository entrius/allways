#!/usr/bin/env bash
# E2E — the v2 money path on a subtensor localnet, against a FRESH deploy of the
# current build. Where spike0.sh proved the chain-extension dust-call, this
# rehearses the whole active-lock + fee-settle lifecycle end to end:
#
#   instantiate(seed=[self]) → post_collateral(0.05τ) → lock_bond
#   → withdraw MUST FAIL while locked
#   → vote_slash(penalty 0.02τ, reimburse 0.01τ to user)   [push-reimburse path]
#   → vote_collect_fees_batch([(self, 0.005τ)])            [batch settle]
#   → vote_unlock(self, epoch 1)
#   → withdraw the EXACT remainder (0.025τ — proves the books balance)
#   → plain transfer 0.005τ to the address                   [burn inventory]
#   → recycle_fees (pot = 0.01 surplus + 0.005 fees + 0.005 donated = 0.02τ)
#
# Miner-facing legs (post/lock/withdraw/status) run through the real `alw vault`
# CLI so this doubles as the CLI smoke test; validator rounds (vote_*) go through
# cargo-contract, since they belong to the validator loop, not the CLI.
#
# The deployer plays every role (owner + sole validator + miner + user), quorum 1.
#
# Setup (same as spike0.sh LOCAL=1):
#   docker run --rm -d -p 9944:9944 --name subtensor-local \
#     ghcr.io/opentensor/subtensor-localnet:latest
#   btcli subnet create/start/register → netuid 2 with a registered hotkey
#   STAKING_HOTKEY=<registered ss58> ./e2e_lock.sh

set -euo pipefail
cd "$(dirname "$0")"

export PATH="$HOME/.cargo/bin:$PATH"
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.89.0-x86_64-unknown-linux-gnu}"

URL="${URL:-ws://127.0.0.1:9944}"
NETUID="${NETUID:-2}"
SURI="${SURI:-//Alice}"
DEPLOYER_SS58="${DEPLOYER_SS58:-5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY}"  # //Alice
STAKING_HOTKEY="${STAKING_HOTKEY:?Set STAKING_HOTKEY to a hotkey registered on netuid $NETUID}"
# Localnet runtime dry-runs are undecodable by cargo-contract 5.x → explicit gas.
GAS_ARGS="--skip-dry-run --gas 300000000000 --proof-size 2000000"
OUT_DIR=".e2e_lock"
mkdir -p "$OUT_DIR"

REPO_ROOT="$(cd ../.. && pwd)"
# --network (global flag) points the CLI's subtensor at the localnet.
ALW="$REPO_ROOT/.venv/bin/python -m allways.cli.main --network $URL"

# Amounts in rao (1 τ = 1e9)
MIN_COLLATERAL=10000000    # 0.01 τ
POST_TAO=0.05              # CLI takes τ; 50000000 rao
PENALTY=20000000           # 0.02 τ seized
REIMBURSE=10000000         # 0.01 τ pushed back to the user (self)
FEE_TOTAL=5000000          # 0.005 τ cumulative protocol fees settled
REMAINDER_TAO=0.025        # 0.05 − 0.02 − 0.005: the books must say exactly this
DONATE_RAW=5000000         # 0.005 τ sent as a bare transfer (no code runs)
VOTE_ROUND_TTL=600   # >= the contract floor of 100
SWAP_REF="0x0202020202020202020202020202020202020202020202020202020202020202"

call() { # call <message> [--value N] [args...]
  local msg="$1"; shift
  # shellcheck disable=SC2086
  cargo contract call --contract "$CONTRACT" --message "$msg" --url "$URL" \
    --suri "$SURI" --execute --skip-confirm $GAS_ARGS "$@"
}

echo "== [1/9] build current code"
cargo contract build --release

echo "== [2/9] instantiate FRESH vault"
# shellcheck disable=SC2086
cargo contract instantiate --url "$URL" --suri "$SURI" \
  --args "$STAKING_HOTKEY" "$NETUID" "$MIN_COLLATERAL" 0 100 "$VOTE_ROUND_TTL" \
         "[$DEPLOYER_SS58]" \
  --execute --skip-confirm $GAS_ARGS --output-json \
  --salt "0x$(python3 -c 'import os;print(os.urandom(8).hex())')" \
  | tee "$OUT_DIR/instantiate.json"
CONTRACT=$(python3 -c "import json;print(json.load(open('$OUT_DIR/instantiate.json'))['contract'])")
echo "$CONTRACT" > "$OUT_DIR/contract_address"
echo "== contract: $CONTRACT"

# CLI env: point alw vault at this deployment, sign as //Alice.
export ALLWAYS_VAULT_ADDRESS="$CONTRACT"
export ALLWAYS_VAULT_SURI="$SURI"
export ALLWAYS_VAULT_METADATA="$(pwd)/target/ink/allways_bond_vault.json"

echo "== [3/9] CLI: post_collateral ${POST_TAO}τ + lock_bond"
$ALW vault post-collateral "$POST_TAO"
$ALW vault lock

echo "== [4/9] withdraw while LOCKED — must be refused"
if $ALW vault withdraw 0.001 2>&1 | tee "$OUT_DIR/locked_withdraw.log" \
   | grep -q "ExtrinsicSuccess.*CollateralWithdrawn"; then
  echo "FAIL: withdraw succeeded on a locked bond"; exit 1
fi
grep -q "rejected the call\|Call failed\|ContractReverted" "$OUT_DIR/locked_withdraw.log" \
  || { echo "FAIL: expected a revert report for locked withdraw"; exit 1; }
echo "-- correctly refused (BondLocked)"

echo "== [5/9] vote_slash: penalty 0.02τ, reimburse 0.01τ (push path) → surplus 0.01τ to pot"
call vote_slash --args "$DEPLOYER_SS58" "$SWAP_REF" "$PENALTY" "$DEPLOYER_SS58" "$REIMBURSE"

echo "== [6/9] vote_collect_fees_batch([(self, 0.005τ)]) — one-entry exit-style batch"
call vote_collect_fees_batch --args "[($DEPLOYER_SS58, $FEE_TOTAL)]"

echo "== [7/9] vote_unlock(self, epoch 1) → CLI: withdraw EXACT remainder ${REMAINDER_TAO}τ"
call vote_unlock --args "$DEPLOYER_SS58" 1
$ALW vault withdraw "$REMAINDER_TAO" | tee "$OUT_DIR/final_withdraw.log"
grep -q "Withdrew" "$OUT_DIR/final_withdraw.log" \
  || { echo "FAIL: exact-remainder withdraw did not succeed — books don't balance"; exit 1; }

echo "== [8/9] plain transfer ${DONATE_RAW} rao to the vault address — no code runs"
# The stranded case the ledger-only pot could never see: a bare balance transfer.
"$REPO_ROOT/.venv/bin/python" - <<PY
import bittensor as bt
sub = bt.subtensor(network="$URL")
call = sub.substrate.compose_call(
    'Balances', 'transfer_keep_alive', {'dest': "$CONTRACT", 'value': $DONATE_RAW}
)
ext = sub.substrate.create_signed_extrinsic(call=call, keypair=bt.Keypair.create_from_uri("$SURI"))
receipt = sub.substrate.submit_extrinsic(ext, wait_for_inclusion=True)
assert receipt.is_success, 'plain transfer to the vault failed'
print('-- plain transfer landed')
PY

echo "== [9/9] CLI: recycle the pot (0.015τ fees/surplus + 0.005τ donated) into SN$NETUID"
$ALW vault recycle --force | tee "$OUT_DIR/recycle.log"
grep -q "Recycle submitted\|pot staked" "$OUT_DIR/recycle.log" \
  || { echo "FAIL: recycle did not submit"; exit 1; }
grep -q "of which donated" "$OUT_DIR/recycle.log" \
  || { echo "FAIL: donated TAO was not swept into the pot"; exit 1; }

echo ""
echo "E2E PASSED — active-lock lifecycle + batch settle + slash reimburse + donation"
echo "sweep + recycle, books balanced to the rao."
echo "Contract: $CONTRACT ($OUT_DIR/contract_address)."

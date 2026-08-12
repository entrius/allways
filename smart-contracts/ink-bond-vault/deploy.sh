#!/usr/bin/env bash
# DEPLOY — instantiate the bond vault with the decided policy numbers.
#
# The vault ships immutable (D7: no set_code_hash), so the constructor arguments
# below are the deployment. They are written here rather than typed at a prompt
# because a wrong `max_collateral` cannot be fixed by redeploying the code.
#
#   min_collateral  0.25 τ  — the least bond that serves one 0.1 τ min swap at the
#                             1.10x guard; must equal Solana's `tao_min_collateral`,
#                             or a miner can lock a bond Solana will not activate.
#   max_collateral  10 τ    — the D7 quorum-compromise bound: a compromised quorum
#                             can fabricate slashes against every bond at once, so
#                             cap the per-miner exposure until the set widens.
#   threshold       66%     — 2-of-3 today; `get_required_votes` rounds up. Below
#                             51 (a strict majority) the instantiation FAILS.
#   vote_round_ttl  600     — blocks (~2 h at 12 s) a vote round stays open.
#   validators      seed set — the ONLY way validators ever come to exist: there
#                             is no owner and no admin key on this contract. An
#                             empty or duplicated set FAILS the instantiation
#                             rather than deploying a vault nobody can ever vote
#                             on. Seed all the operators you have; the set can
#                             only grow afterwards by unanimous vote.
#
# All four numbers are changeable after deploy only by a UNANIMOUS validator round
# (`alw vault admin set-config`), as are the staking hotkey and netuid (`alw vault
# admin set-recycle-target`); the point of seeding them correctly is that the vault
# is never live at a wrong value.
#
# Usage:
#   STAKING_HOTKEY=<ss58 registered on NETUID> URL=wss://entrypoint-finney.opentensor.ai:443 \
#     SURI="<deployer seed>" ./deploy.sh
#
# After it prints the address:
#   1. alw vault admin add-validator <ss58>   — unanimous vote; the candidate then
#                                               runs `alw vault admin accept` WITHIN
#                                               vote_round_ttl blocks of the approving
#                                               vote, and before the set changes again
#                                               — the approval expires otherwise and
#                                               the set must vote them in afresh
#   2. alw vault status                        — confirm bounds, threshold, validator set
#   3. alw config set vault-address <address>  — and ship it to every validator's env
#   4. record the address + code hash in SOLANA_BITTENSOR_SPLIT_COLLATERAL.md

set -euo pipefail
cd "$(dirname "$0")"

export PATH="$HOME/.cargo/bin:$PATH"
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.89.0-x86_64-unknown-linux-gnu}"

URL="${URL:?Set URL to the target chain endpoint (e.g. wss://entrypoint-finney.opentensor.ai:443)}"
# Prompt silently if not passed — keeps the deployer seed out of shell history and the env of the
# calling shell. It still reaches cargo-contract as --suri (unavoidable); this only removes history.
if [ -z "${SURI:-}" ]; then
  read -rs -p "Deployer seed (hidden — the account that pays the deploy, nothing more): " SURI
  echo
fi
[ -n "${SURI:-}" ] || { echo "no deployer seed provided; aborting"; exit 1; }
NETUID="${NETUID:-7}"
STAKING_HOTKEY="${STAKING_HOTKEY:?Set STAKING_HOTKEY to a hotkey registered on netuid $NETUID}"

# Amounts in rao (1 τ = 1e9). Overridable, but every override is a policy change.
MIN_COLLATERAL="${MIN_COLLATERAL:-250000000}"     # 0.25 τ
MAX_COLLATERAL="${MAX_COLLATERAL:-10000000000}"   # 10 τ (0 would mean UNLIMITED)
CONSENSUS_THRESHOLD="${CONSENSUS_THRESHOLD:-66}"  # percent (contract floor: 51)
VOTE_ROUND_TTL="${VOTE_ROUND_TTL:-600}"           # blocks (contract floor: 100)
# Comma-separated ss58 seed validator set. One is valid (the bootstrap case), but
# note n=2 is the ONLY configuration worse than the one before it: both members
# are required for every quorum, so one dark key freezes every locked bond with
# no owner left to repair it. Do not hold locked bonds while below 3.
SEED_VALIDATORS="${SEED_VALIDATORS:?Set SEED_VALIDATORS to a comma-separated ss58 list}"

# Set GAS_ARGS="--skip-dry-run --gas ... --proof-size ..." on runtimes whose
# dry-runs cargo-contract cannot decode (localnet 425; see spike0.sh).
GAS_ARGS="${GAS_ARGS:-}"
OUT_DIR=".deploy"
mkdir -p "$OUT_DIR"

echo "== target:         $URL (netuid $NETUID)"
echo "== staking_hotkey: $STAKING_HOTKEY"
echo "== bond bounds:    min $MIN_COLLATERAL rao / max $MAX_COLLATERAL rao"
echo "== quorum:         $CONSENSUS_THRESHOLD%, round TTL $VOTE_ROUND_TTL blocks"
echo "== seed set:       $SEED_VALIDATORS"
read -r -p "Instantiate an IMMUTABLE vault with these values? [y/N] " ok
[ "$ok" = "y" ] || { echo "aborted"; exit 1; }

echo "== [1/2] cargo contract build --release"
cargo contract build --release

echo "== [2/2] instantiate"
# shellcheck disable=SC2086
cargo contract instantiate --url "$URL" --suri "$SURI" \
  --args "$STAKING_HOTKEY" "$NETUID" "$MIN_COLLATERAL" "$MAX_COLLATERAL" \
         "$CONSENSUS_THRESHOLD" "$VOTE_ROUND_TTL" "[$SEED_VALIDATORS]" \
  --execute --skip-confirm $GAS_ARGS --output-json \
  | tee "$OUT_DIR/instantiate.json"

CONTRACT=$(python3 -c "import json;print(json.load(open('$OUT_DIR/instantiate.json'))['contract'])")
echo "$CONTRACT" > "$OUT_DIR/contract_address"

echo ""
echo "VAULT DEPLOYED: $CONTRACT"
echo "Owner is the SURI signer. Next: add the validator hotkeys, then publish the address."

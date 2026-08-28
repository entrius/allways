# Allways

**Settlement layer for agents and applications**

Native cross-chain transactions for programs that hold one asset and need to pay in another — no wrapped tokens, no bridges, no custodian. Bittensor Subnet 7 (SN7).

[![Twitter](https://img.shields.io/twitter/follow/allways_io?style=social)](https://x.com/allways_io)

## Overview

Allways is a settlement layer built to be driven by software. An agent or application that holds SOL, TAO, BTC, or any supported asset submits a single swap and receives the destination asset natively in its own wallet — no account, no custodian, no bridge in the path. Allways creates a verification layer above independent systems: miners complete transactions, validators independently verify both legs on-chain, and a smart contract enforces outcomes through collateral and slashing.

Currently live with SOL and TAO as hubs, each paired against BTC, ETH, USDC-on-Arbitrum, HYPE, BNB, AVAX, USDC-on-Base, USDC-on-Ethereum, CRO, ASTER, UNI, QNT, POL, USDC-on-Polygon, PAXG, and USDC-on-Solana — plus SOL ↔ TAO itself (hub-and-spoke: every pair has a SOL or TAO leg). Designed to scale to any verifiable asset.

## For agents

Allways is designed to be operated by software, not clicked through by people. Every step of a swap — quote discovery, reservation, deposit, and settlement — is a CLI command (`alw swap now`) or a public API call (`api.all-ways.io`) with structured output, so an autonomous agent can clear a payment in another native asset as a single tool call, with no human in the loop, no exchange account, and no custodian holding its keys. This is how the network is used in practice: agents operated by the team and by users originate swaps today, and the miner and validator neurons in this repo are themselves unattended programs that quote, fulfill, and verify around the clock.

**Why agents need a settlement layer.** An agent's wallet is a single-chain identity, but the things it pays for are not. An LLM agent earning TAO may need to buy inference from a provider that bills in USDC; a trading agent holding SOL may need to settle an obligation in BTC; a multi-agent pipeline may split revenue across operators who each want a different native asset. Bridges and centralized exchanges break the autonomy model — they require accounts, KYC, custody, and a human to unblock them. Allways lets the agent stay self-custodial: it sends the source asset from its own wallet and receives the destination asset in its own wallet, and the protocol verifies the outcome on-chain.

**Swap lifecycle as tool calls.**

- **Discover**: `GET` live quotes per pair from the API and select a rate, liquidity, and miner.
- **Reserve**: lock that miner's quote and collateral for the swap window.
- **Deposit**: send the source asset natively from the agent's wallet; validators attest the deposit on-chain.
- **Settle**: the miner delivers the destination asset to the agent's wallet; validators verify the delivery or slash the miner's collateral to reimburse the agent.

Each call returns machine-readable state (swap id, reservation window, deadlines, attestation status), so an agent can plan, retry, and reason over the full lifecycle without parsing prose.

**Use cases in production and in reach.**

- **Agent-to-agent payments**: an orchestrator pays sub-agents or tool providers in whatever asset they accept, funded from a single treasury.
- **Inference and compute procurement**: convert earned TAO or SOL into the stablecoin or native token a GPU or model provider bills in.
- **Treasury automation**: an agent rebalances a multi-chain treasury on a schedule or on a signal, without moving funds through a custodian.
- **Autonomous market-making**: the reference miner is itself an agent — it posts quotes, manages collateral, and fulfills swaps programmatically; operators extend it with their own pricing and risk logic.
- **Bittensor-native economics**: agents earning alpha or TAO on other subnets settle into the asset they actually spend, with SOL and TAO as hubs.

See the Swap guide at [docs.all-ways.io](https://docs.all-ways.io/) for the full lifecycle and API reference.

## Miner Risk Disclaimer

The miner in this repository is **reference software**. Review the code thoroughly and build it out with your own safety and optimization measures before running it. Running the base miner, or anything you build on top of it, is at your own risk.

## Getting Started

### Requirements

- Python 3.10+
- Bittensor wallet
- Docker & Docker Compose

### Installation

### Running with Docker

**Miner:**

```bash
docker compose -f docker-compose.miner.yml up -d
```

**Validator:**

```bash
docker compose -f docker-compose.vali.yml up -d
```

Both require a `.env` file with `PORT` and `WALLET_PATH` configured.

### CLI

```bash
uv sync
# activate the uv virtual environment
source .venv/bin/activate

alw --help
```

## Architecture

- **Miners**: Post exchange rate pairs and collateral, fulfill swap orders
- **Validators**: Monitor swaps, verify on-chain transactions, vote on outcomes
- **Smart Contract**: Manages collateral, swap lifecycle, and validator voting
- **CLI**: User interface for posting pairs, managing collateral, and executing swaps

## Miner Onboarding

Bond, then activate, then quote — in that order, for either backing. A quote is a promise that
one specific bond answers for, so `set_quote` refuses a purse you are not already serving
(`MinerNotActive`). Quoting before activation is rejected, not queued.

**SOL-backed** (collateral held on Solana):

```bash
alw collateral deposit <SOL>                   # fund the local purse (bind-hotkey needs it — see below)
alw miner bind-hotkey                          # bind your hotkey to your Solana pubkey (once)
alw miner activate                             # validators vote you active
alw miner post sol <addr> btc <addr> <rate>    # quote
```

**TAO-backed** (bond held in the Bittensor vault). Same order; the bond lives on another chain,
so activation waits on validators mirroring it to Solana rather than on a local read:

```bash
alw collateral deposit 0.1                     # one-time identity deposit — see the note below
alw miner bind-hotkey                          # the vault keys bonds by hotkey, joined via this binding
alw vault post-collateral <TAO>                # bond into the vault (signed by the hotkey)
alw vault lock                                 # enter service — only a LOCKED bond is attested
                                               # wait a minute: validators mirror the bond to Solana
alw miner activate --backing tao               # validators vote that purse active
alw miner post sol <addr> tao <addr> <rate> --backing tao
```

Purses activate one at a time, so `alw miner activate` lights one. It infers the backing when only
one purse is funded and not yet serving — which is every step of the order above — and asks for
`--backing` only when both are candidates at once. Activation is refused, not queued, while the
bond has yet to be mirrored: retry rather than wait on the request.

`alw miner status` shows the required bond and whether each purse is serving yet.

**A TAO-only miner still posts a small SOL deposit — once.** `bind-hotkey` requires a live local
collateral stake (`min_collateral`, currently 0.1 SOL) — which is why the deposit comes first in both
recipes above — because binding a hotkey is what claims that identity on Solana and the deposit is the
anti-squat cost of the claim. Since the vault keys bonds by
hotkey and validators join them to your Solana pubkey through that binding, a TAO-backed miner needs
the binding to set rates or be credited for its swaps — so it needs the deposit too. That is the whole
of it: the SOL purse never has to be activated, it posts no quotes, and it backs nothing. Withdraw it
by deactivating and waiting out the cooldown, the same as any SOL collateral.

**What a TAO-backed quote guarantees.** If the miner fails to deliver, the user is reimbursed in
TAO from the miner's bond, shortly after the timeout. That differs from a SOL-backed quote in
timing only: a SOL refund is instant because the collateral sits beside the swap, while the TAO
reimbursement waits for validators to carry the timeout verdict to the vault and reach quorum
there. Either way the user is made whole out of the bond that backed the quote.

**Leaving.** A locked bond is not withdrawable on demand. Deactivate the purse
(`alw miner deactivate --backing tao`), let in-flight swaps and their timeout windows drain, and
validators unlock the bond once nothing is owed on it — then `alw vault withdraw` succeeds.

## Validator Storage Layout

Validator state lives in `~/.allways/validator/state.db` (SQLite, WAL mode).
Tables: `pending_confirms`, `rate_events`, `swap_outcomes`. Collateral /
active / min_collateral state is held in memory and rebuilt from contract
events each startup; only `swap_outcomes` (the all-time credibility ledger)
needs to persist across restarts.

## Miner Environment Variables

- `BTC_PRIVATE_KEY`, `ETH_PRIVATE_KEY`, `ARB_PRIVATE_KEY`, `HYPE_PRIVATE_KEY`, `BNB_PRIVATE_KEY`, `AVAX_PRIVATE_KEY`, `BASE_PRIVATE_KEY`, `CRO_PRIVATE_KEY`, `POL_PRIVATE_KEY`, `{ETH,ARB,HYPE,BNB,AVAX,BASE,CRO,POL}_RPC_URLS`, etc. — keyed by network, so assets sharing one share its config (ETH, ethusdc, uni, qnt and paxg all ride the `ETH_*` vars; BNB and aster both ride the `BNB_*` vars). See `.env.example`.

## Running a Local Subtensor Lite Node (Validators)

Validators read miner rate commitments every ~3 minutes AND stream contract
events every block via the same connection. Pointing at the public `finney`
entrypoint works but adds latency and RPC pressure — every validator on the
network should run its own lite node for this.

```bash
# Minimal lite-node command (adjust --base-path for storage volume)
subtensor \
  --chain finney \
  --base-path /var/lib/subtensor \
  --rpc-external \
  --ws-external \
  --port 30333 \
  --rpc-port 9933 \
  --ws-port 9944 \
  --pruning 1000
```

Then point the validator at it via `.env`:

```env
SUBTENSOR_NETWORK=ws://127.0.0.1:9944
```

The dev environment in `alw-utils/dev-environment` provisions a local chain
automatically — no manual lite-node step is required there.

## License

MIT License

---

<sub>Allways is permissionless, open-source, beta software. Swaps settle directly between counterparty wallets; the protocol never takes custody of user funds, and the protocol fee is charged against miner collateral rather than any user transfer. Validator operators, including those run by the project, verify swap outcomes but cannot redirect or receive any transferred amount. Use at your own risk. This software is provided "as is" without warranty of any kind. Nothing herein constitutes financial advice, and the creators assume no liability for losses arising from use of the protocol.</sub>

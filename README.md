# Allways

**Universal Transaction Layer**

Trustless native transactions across independent assets — Bittensor Subnet 7 (SN7).

## Overview

Allways creates a verification layer above independent systems. Assets move natively. Miners complete transactions, validators independently verify the results, and a smart contract enforces outcomes through collateral and slashing.

Currently live with BTC ↔ TAO. Designed to scale to any verifiable asset.
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

## Running a Local Subtensor Lite Node (Validators)

Validators read miner rate commitments every ~3 minutes. Pointing at the public
`finney` entrypoint works but adds latency and RPC pressure — every validator on
the network should run its own lite node for this.

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

<sub>Allways is permissionless, open-source, beta software. The protocol facilitates trustless peer-to-peer transactions — the creators and contributors do not custody, control, or intermediate any funds. Use at your own risk. This software is provided "as is" without warranty of any kind. Nothing herein constitutes financial advice, and the creators assume no liability for losses arising from use of the protocol.</sub>

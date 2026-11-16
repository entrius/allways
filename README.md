# Allways

Bittensor Subnet 7 (SN7).

**Links:** [Website](https://all-ways.io) · [Docs](https://docs.all-ways.io)

## What this repository is now

This repository holds the SN7 validator and nothing else. Every epoch the validator puts 100% of its
weight on one pinned UID, the Allways treasury miner. It does no scoring, serves no axon, and talks to
no chain other than Bittensor.

The previous code (cross-chain swap miner and validator, the `alw` CLI, the smart contracts) is
preserved on the [`allways-old`](https://github.com/entrius/allways/tree/allways-old) branch.

## How the target is chosen

`allways/constants.py` pins the target:

| Constant          | Meaning                                                                    |
| ----------------- | -------------------------------------------------------------------------- |
| `TREASURY_UID`    | The UID that receives the full weight. `None` until the miner is registered |
| `TREASURY_HOTKEY` | The hotkey expected at that UID. A guard, not a lookup                      |
| `BURN_UID`        | The fallback target: the subnet owner hotkey's UID, whose weight is burned  |

On every pass the validator syncs the metagraph and sets weights `[1.0]` on:

- `TREASURY_UID`, when it is set, in range, and the hotkey registered there is `TREASURY_HOTKEY`
- `BURN_UID` otherwise, with a `TREASURY FALLBACK` warning that says why (unset, out of range, or
  hotkey mismatch)

The hotkey check stops emissions going to a stranger if the UID slot is ever recycled.

Weights are set once the validator's last update is more than `--neuron.epoch_length` blocks old
(default 150), and never more often than the subnet's `weights_rate_limit`. Commit-reveal is handled
by the Bittensor SDK. Any chain error drops the connection and reconnects on the next pass.

## Running a validator

Requirements: a hotkey registered on SN7 with a validator permit. No GPU, and no open ports.

### Docker (recommended)

```bash
cp .env.example .env          # set WALLET_NAME, HOTKEY_NAME, WALLET_PATH
docker compose -f docker-compose.vali.yml up -d
docker logs -f aw-vali
```

The compose file runs `entrius/allways:latest` with watchtower, so the validator updates itself when
a new image is published. An `.env` and compose file from the previous validator keep working:
variables and flags that no longer apply are ignored.

### From source

```bash
uv sync
uv run python neurons/validator.py --netuid 7 --subtensor.network finney \
  --wallet.name <coldkey> --wallet.hotkey <hotkey> --logging.info
```

### Check without submitting

```bash
uv run python neurons/validator.py --netuid 7 --subtensor.network finney --dry-run
```

`--dry-run` needs no wallet. It logs the weights that would be set and exits. `VALIDATOR_MODE=watch`
(or `--neuron.disable_set_weights`) runs the normal loop but only logs.

## Development

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run pytest tests/ -q
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT, see [LICENSE](LICENSE).

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

| Constant          | Meaning                                                                     |
| ----------------- | --------------------------------------------------------------------------- |
| `NETUID_FINNEY`   | The subnet the treasury constants are pinned for (7)                        |
| `TREASURY_UID`    | The UID that receives the full weight (78). `None` burns everything instead |
| `TREASURY_HOTKEY` | The hotkey expected at that UID. A guard, not a lookup                      |

On every pass the validator syncs the metagraph and sets weights `[1.0]` on:

- `TREASURY_UID`, when the netuid is 7 and the UID is set, in range, and held by `TREASURY_HOTKEY`
- otherwise the UID where the subnet owner hotkey is registered, read from the chain on each pass.
  Weight placed there is burned. A `TREASURY FALLBACK` warning says why (another netuid, unset, out
  of range, or hotkey mismatch)
- nothing at all, with a `NO WEIGHTS SET` error, if the treasury cannot be used and the subnet owner
  hotkey is not registered on the subnet. There is no hard-coded fallback UID

The hotkey check stops emissions going to a stranger if the UID slot is ever recycled.

Weights are set once the validator's last update is more than `--neuron.epoch_length` blocks old
(default 150), and never more often than the subnet's `weights_rate_limit`. Commit-reveal is handled
by the Bittensor SDK. A validator without a validator permit logs an error and submits nothing. Any
chain error drops the connection and reconnects on the next pass.

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

`--dry-run` needs no wallet. It logs the weights that would be set and exits (exit code 1 if there
is no safe target). `--neuron.disable_set_weights` runs the normal loop but only logs.

## Development

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run pytest tests/ -q
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT, see [LICENSE](LICENSE).

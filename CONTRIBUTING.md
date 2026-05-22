# Contributing to Allways

## Development Setup

1. Clone the repository
2. Install dependencies: `uv sync`
3. Install pre-commit hooks: `uv run pre-commit install --install-hooks`
4. Run tests: `uv run pytest tests/`
5. Lint: `uv run ruff check allways/ neurons/`

## Git Hooks

This project uses [pre-commit](https://pre-commit.com/) for automated code quality checks.

**Pre-commit hooks** (run on every commit):
- Trailing whitespace removal
- End-of-file fixer
- YAML/JSON validation
- Line ending normalization (LF)
- Large file detection (>500KB)
- Ruff linting (with auto-fix)
- Ruff formatting

**Pre-push hooks** (run before push):
- Pytest test suite

### Running hooks manually

```bash
uv run pre-commit run --all-files                        # all pre-commit hooks
uv run pre-commit run --all-files --hook-stage pre-push  # pre-push hooks
```

## Code Style

- Line length: 120 characters
- Single quotes for strings
- Ruff linting with E, F, I rules

## Pull Requests

1. Create a feature branch
2. Make your changes
3. Ensure tests pass
4. Submit a pull request

## Automatic Closures

The maintainer bot enforces these rules without manual review. Contributions that violate them are closed automatically.

### Open item limits

Each contributor may have at most **2 open PRs** and **2 open issues** in this repository at any time. Submitting a 3rd of either type while at the cap closes the new one on submission. The limits apply independently — you can have 2 open PRs and 2 open issues at the same time.

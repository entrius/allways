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

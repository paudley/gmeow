# Contributing

## Development Setup

```bash
uv sync --extra test
cp config.toml-example config.toml
```

Use a disposable database for integration testing. Do not point tests at a production mailbox database.

## Checks

```bash
uv run python -m compileall main.py src migrations tests
uv run pytest
uv build
uv run python scripts/public_release_check.py
```

## Pull Requests

- Keep changes focused.
- Do not commit `config.toml`, secrets, mailbox exports, or runtime data.
- Add or update tests for behavior changes.
- Update `README.md` or docs when public behavior changes.

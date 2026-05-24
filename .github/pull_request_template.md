## Summary

## Verification

- [ ] `uv run python -m compileall main.py src migrations tests`
- [ ] `uv run pytest`
- [ ] `uv run python scripts/public_release_check.py`

## Data Safety

- [ ] No `config.toml`, secrets, mailbox exports, or runtime data are included.

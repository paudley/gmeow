# Public Release Checklist

Run this before pushing a public release branch:

- `uv sync --extra test`
- `uv run python -m compileall main.py src migrations tests`
- `uv run pytest`
- `uv build`
- `uv run python scripts/public_release_check.py`
- `git -c core.whitespace=blank-at-eol,blank-at-eof,space-before-tab diff --check`

Manual checks:

- `config.toml` is ignored and not tracked.
- `config.toml-example` is sanitized and uses `[gmeow]`.
- No service-account keys, OAuth client IDs, mailbox addresses, local home paths, production database credentials, or mailbox data are tracked.
- Wheel and sdist do not include runtime data or local secrets.
- GitHub secret scanning, push protection, Dependabot, and code scanning are enabled in repository settings where available.

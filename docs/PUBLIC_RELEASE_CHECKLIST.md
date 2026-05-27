# Public Release Checklist

Run the canonical local quality gate before pushing a public release branch:

- `make check`

Manual checks:

- `gmeow.toml` is ignored and not tracked.
- `gmeow.toml-example` is sanitized and uses the current Go runtime schema.
- Go release artifacts are binaries/containers, not PyPI packages.
- PyPI publication is limited to `python/` (`gmeow-intel`).
- No service-account keys, OAuth client IDs, mailbox addresses, local home paths, production database credentials, or mailbox data are tracked.
- Wheel and sdist do not include runtime data or local secrets.
- GitHub secret scanning, push protection, Dependabot, and code scanning are enabled in repository settings where available.

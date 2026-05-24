# Repository Maintenance Scripts

This package collects one-off utilities that operate on the repository itself rather than on a
running Gmeow instance. They are invoked from the repo root via `python -m scripts.<name>` or
through Makefile targets.

## Layout

- `public_release_check.py` — Pre-publication scanner that asserts no private mailbox data,
  credentials, machine paths, or production identifiers leak into tracked files. Run as part of
  the `make release-check` target before each public-facing release.

## Conventions

- Scripts must stay idempotent and side-effect-free on the live database; they operate on the
  source tree, not on the cache.
- New scripts should ship with a docstring describing the operator workflow and an entry in this
  document.

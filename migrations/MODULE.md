# Alembic Migrations

This package hosts the Alembic environment and revision history for the Gmeow PostgreSQL
schema. Apply migrations through `make migrate` (or `alembic upgrade head` with the
local DSN configured) before running the server or the integration tests.

## Layout

- `env.py` — Alembic environment hook; reads the DSN from `config.toml` via the bootstrap helper
  in `gmeow.db`.
- `script.py.mako` — Alembic template used when generating new revisions.
- `versions/` — committed migration revisions.

## Conventions

- Revisions are written by hand against the canonical `GmeowConfig.postgres_dsn`. Never edit a
  committed revision after it has been applied to a shared environment; add a new revision instead.
- The pgvector and Apache AGE extensions are expected to exist on the target database. The
  migrations create the application schema; database-level extension installation is an operator
  prerequisite.

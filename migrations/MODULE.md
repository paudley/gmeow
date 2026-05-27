# Migrations

This directory hosts Go-owned goose migrations for rebuildable PostgreSQL projections.
The retired Python Alembic runtime and revision history have been removed.

## Layout

- `query/` — QUERY projection migrations applied by `gmeow-admin query migrate`.

## Conventions

- Migrations are written by hand against the greenfield Go QUERY schema. Never edit a committed
  migration after it has been applied to a shared environment; add a new migration instead.
- The pgvector and Apache AGE extensions are expected to exist on the target database. The
  migrations create projection schema only; database-level extension installation is an operator
  prerequisite.

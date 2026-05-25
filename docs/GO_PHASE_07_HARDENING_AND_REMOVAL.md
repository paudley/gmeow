# Go Migration Phase 07 - Hardening, Operations, And Final Python Removal

## Goal

Prove the Go system is operationally durable, self-healing, and independent of the old Python
application. This phase removes remaining Python runtime, packaging, migration, and test artifacts
only after previous phases have retired their behavior.

## Build

- Complete `gmeow-admin filestore verify`.
- Complete `gmeow-admin query rebuild`.
- Complete scheduler repair scans.
- Complete dead-letter inspection and requeue tools.
- Add backup/restore documentation for FILESTORE authority.
- Add metrics for queue depth, analysis latency, projection lag, corrupt objects, failed analyzers,
  source errors, and interface latency.
- Add load tests for ingest, projection, analysis, and search.
- Add chaos tests for worker crash, broker restart, PostgreSQL wipe, corrupt object, and interrupted
  annotation writes.
- Add production-safety guards for destructive commands.

## Retire Python Equivalent

Remove any remaining Python application scaffolding only when no phase still depends on it:

- `src/gmeow/`
- `tests/` Python-only tests replaced by Go/functional tests
- `migrations/` Alembic files replaced by goose migrations
- old Python package metadata in root `pyproject.toml`
- old PyPI publishing workflow for `gmeow`
- Python install/run instructions in README
- Python cache/build artifacts

If Python remains for a deliberately retained external analyzer, it must live outside the old app
package shape and be documented as an analyzer runtime adapter, not as the gmeow application.

## Functional Proof

- A fresh checkout can build and run the Go binaries without installing the old Python package.
- FILESTORE verify reports corruption without poisoning unrelated reads or projection.
- PostgreSQL can be wiped and rebuilt from FILESTORE.
- RabbitMQ restart redelivers unacked jobs.
- Dead-letter jobs can be inspected and requeued through admin commands.
- Backup/restore documentation has been exercised against a temporary restore target.
- Canonical quality gate passes.

## Exit Gate

- No old Python application package remains.
- No operator documentation points at the Python runtime.
- No release workflow publishes the old Python app.
- Go binaries and docs are the only supported application distribution path.

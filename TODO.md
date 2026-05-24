# Gmeow Active TODO

## Active Queue

- [x] Fix review findings for category filtering and backfill dead-job advancement.
  - [x] Keep include category predicates in SQL before `LIMIT` for text search.
  - [x] Keep include category predicates in SQL before `LIMIT` for address results.
  - [x] Prevent backfill checkpoint advancement when target intelligence jobs are dead.
  - [x] Add focused regression tests and rerun focused suite.
- [x] Public release prep: add MIT license, SPDX headers, sanitized docs, GitHub community/security files, CI, Dependabot, and release hygiene checks.
- [x] Convert config to TOML-only `config.toml` with all app settings under `[gmeow]`.
- [x] Create sanitized `config.toml-example` and keep local `config.toml` ignored.
- [x] Remove committed YAML config files and all runtime/docs references to the old YAML config path.
- [x] Remove PII/private provisioning values from tracked docs, examples, tests, and MCP help text.
- [x] Tighten `pyproject.toml` metadata so the project is PyPI-ready for a near-future release.
- [x] Verify compile, tests, build, release hygiene, and whitespace checks.
- [x] Move local credentials/secrets to configured SOPS file `~/.config/gmeow/secrets.sops.yaml` and read them through `[gmeow.secrets]`.
- [x] Implement oldest-first Gmail backfill with monthly capped batches, batch-complete analysis, crash-safe checkpoints, validation mode, and minimal repeat work.
  - [x] Add paged Gmail search and lightweight metadata fetch support.
  - [x] Add backfill batch orchestration with `sync_state` checkpoints and completion checks.
  - [x] Expose backfill through CLI, REST, MCP, Makefile, and scheduled maintenance.
  - [x] Add focused tests for paging, resume, validation, skip-complete, and failure behavior.
  - [x] Verify compile and focused test suite.
- [x] Diagnose slow live backfill throughput and adjust default/server behavior so unattended backfill makes meaningful progress.
  - [x] Confirm live scheduler throughput, failures, corpus size, and current backfill cadence.
  - [x] Reduce default unattended backfill idle time while preserving complete-per-batch analysis.
  - [x] Add a default-off scheduled backfill enable knob and enable it only in local config.
  - [x] Restart server and verify backfill continues without fresh errors.
- [x] Repair backfill implementation quality issues found by coding-ethos checks.
  - [x] Restore CLI/API/MCP backfill entry points.
  - [x] Remove banned future annotations, Optional-style annotations, and noqa workarounds from touched code.
  - [x] Tighten SyncService, maintenance, resilience, and Gmail client typing surfaces.
  - [x] Add googleapiclient stubs for strict Pyright without runtime import tricks.
  - [x] Verify focused Ruff and Pyright checks.

## Archived

Verified-complete TODO and plan items were moved to `ARCHIVE.md`.

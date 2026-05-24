# Gmeow Active TODO

## Active Queue

- [x] Public release prep: add MIT license, SPDX headers, sanitized docs, GitHub community/security files, CI, Dependabot, and release hygiene checks.
- [x] Convert config to TOML-only `config.toml` with all app settings under `[gmeow]`.
- [x] Create sanitized `config.toml-example` and keep local `config.toml` ignored.
- [x] Remove committed YAML config files and all runtime/docs references to the old YAML config path.
- [x] Remove PII/private provisioning values from tracked docs, examples, tests, and MCP help text.
- [x] Tighten `pyproject.toml` metadata so the project is PyPI-ready for a near-future release.
- [x] Verify compile, tests, build, release hygiene, and whitespace checks.
- [x] Move local credentials/secrets to configured SOPS file `~/.config/gmeow/secrets.sops.yaml` and read them through `[gmeow.secrets]`.

## Archived

Verified-complete TODO and plan items were moved to `ARCHIVE.md`.

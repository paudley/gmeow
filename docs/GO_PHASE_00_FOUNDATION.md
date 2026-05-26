# Go Migration Phase 00 - Foundation And Contracts

## Goal

Create the greenfield Go workspace, shared contracts, config loader, and quality gates before moving
runtime behavior. This phase proves the new project can start, validate configuration, and expose
stable interfaces without depending on the Python application.

## Build

- Add Go module layout and binaries:
  - `cmd/gmeow`
  - `cmd/gmeow-admin`
  - `cmd/gmeow-worker`
  - `internal/config`
  - `internal/contracts`
  - `internal/appsvc`
  - `internal/filestore`
  - `internal/query`
  - `internal/scheduler`
  - `internal/analysis`
  - `internal/source`
  - `internal/interface`
- Define protocol-shaped Go interfaces for FILESTORE, QUERY, SCHEDULER, ANALYSIS, SOURCE, and
  application services.
- Define shared JSON schemas or Go structs for manifests, facets, provenance, relationships,
  source events, analyzer specs, analyzer jobs, annotations, search requests, and search results.
- Implement the shared config parser:
  - one parser for every binary;
  - SOPS age identity required from `GMEOW_SOPS_UNLOCK_KEY` or `~/.config/gmeow/key.txt`;
  - no `secrets.file` or secondary config pointer inside `gmeow.toml`;
  - encrypted SOPS JSON leaf secrets only;
  - startup validation cannot be disabled.
- Add `gmeow-admin config validate`.
- Add `gmeow-admin config secret set`, `unset`, and `list` command stubs with validated behavior
  before wiring real encrypted writes.
- Document the canonical quality gate for Go and any transitional Python tests still present.

## Retire Python Equivalent

Remove Python configuration and bootstrap behavior only after the Go config path passes functional
tests:

- `src/gmeow/config.py`
- config-specific tests in `tests/_test_config.py`
- README sections that instruct operators to configure the Python app as the primary runtime

Do not remove modules that still provide behavior for later phases. Each later phase owns its own
Python retirement.

## Functional Proof

- `gmeow-admin config validate` fails without the SOPS unlock key.
- `gmeow-admin config validate` accepts a minimal valid config with encrypted leaf secrets.
- A config with `secrets.file` or `config.file` is rejected.
- All binaries start far enough to validate config and print version/status without initializing
  unimplemented components.
- The quality gate runs from one documented command.

## Exit Gate

- Go packages compile.
- Dependency direction is enforced by package boundaries.
- The config parser is the only config/SOPS reader.
- No Go component receives raw unresolved secret references.
- Python config/bootstrap code is removed only after the Go path is the documented operator path.

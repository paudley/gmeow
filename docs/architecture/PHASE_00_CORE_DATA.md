# Phase 00 Core Data Assumptions

Phase 00 does not start PostgreSQL, RabbitMQ, FILESTORE, QUERY, SOURCE, INTERFACE, or ANALYSIS
runtime components. It only establishes the config, contract, package, and quality-gate surfaces
that later phases use.

Development assumptions for later core-data phases:

- PostgreSQL and RabbitMQ are external services, preferably started through a local container stack.
- FILESTORE state lives under the resolved `system.data_dir` and `filestore.root`.
- Config validation always runs before any component connects to PostgreSQL, RabbitMQ, source APIs,
  or filesystem storage.
- Secrets are named encrypted leaves in the selected SOPS-protected config; components receive
  resolved values only.
- Python ANALYSIS workers receive resolved job/spec payloads and never read `gmeow.toml` directly.

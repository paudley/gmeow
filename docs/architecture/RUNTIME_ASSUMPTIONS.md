# Runtime Core Data Assumptions

Gmeow runtime components assume PostgreSQL and RabbitMQ are external services,
FILESTORE owns durable object state, and config validation runs before service
startup. These assumptions are shared by local development, tests, and systemd
production operation.

Core data assumptions:

- PostgreSQL and RabbitMQ are external services, preferably started through a local container stack.
- FILESTORE state lives under the resolved `system.data_dir` and `filestore.root`.
- Config validation always runs before any component connects to PostgreSQL, RabbitMQ, source APIs,
  or filesystem storage.
- Secrets are named encrypted leaves in the selected SOPS-protected config; components receive
  resolved values only.
- External ANALYSIS adapters receive resolved job/spec payloads and never read `gmeow.toml` directly.

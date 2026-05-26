<p align="center">
  <img src="https://raw.githubusercontent.com/paudley/gmeow/main/docs/gmeow-logo.svg" alt="Gmeow logo" width="280">
</p>

# Gmeow

FILESTORE-first local knowledge services for agents.

Gmeow is in a Go rewrite. Phases 0-6 provide the greenfield runtime: shared contracts, SOPS-backed config validation, FILESTORE authority, rebuildable PostgreSQL QUERY projection, RabbitMQ-backed SCHEDULER work derivation, Go ANALYSIS worker runtime, Go SOURCE adapters, shared application services, and MCP/REST/read-only IMAP interfaces.

Gmeow is designed for trusted single-user local systems. By default it binds to `127.0.0.1` and does not add application-level authentication. Do not expose it directly to an untrusted network.

## Current Go Runtime

- Validates one shared `gmeow.toml` config path for all Go binaries.
- Stores authoritative content and annotations in FILESTORE object directories.
- Projects FILESTORE manifests into PostgreSQL QUERY tables, pgvector rows, and Apache AGE graph state.
- Derives analysis work through SCHEDULER and publishes durable RabbitMQ jobs with retry and dead-letter handling.
- Runs typed gRPC service endpoints for FILESTORE, QUERY, and SCHEDULER over Unix sockets by default.
- Provides admin commands for config, FILESTORE verification, QUERY projection, and SCHEDULER operations.
- Runs Go ANALYSIS workers that consume scheduler jobs, read/write FILESTORE through typed gRPC, and support explicit `gmeow-intel` external analyzer adapters for Python/model behavior.
- Provides Go SOURCE adapters for local filesystem fixtures, push/ringme records, the Gmail SOURCE adapter for ingest/hydrate/live search/live retrieve/actions, and design-only Drive capability checks.
- Exposes shared application services through `gmeow mcp-serve`, `gmeow rest-serve`, and read-only `gmeow imap-serve`; user-facing `search`, `mail-search`, `retrieve`, `ops-status`, and `force-analysis` commands use the same service layer.

## Features

- BLAKE3 content identity with zstd-compressed FILESTORE blobs and immutable recovery sidecars.
- Atomic manifest, analysis, overlay, and source-cursor annotations.
- Rebuildable PostgreSQL projection for facets, provenance, relationships, compound parts, analysis status, graph facts, keywords, embeddings, overlays, and source cursors.
- Read-only Apache AGE graph inspection over projected graph facts.
- Go SCHEDULER work derivation with RabbitMQ priority, retry, and dead-letter queues.
- Go SOURCE adapters submit normalized content to FILESTORE and use source lookup/ingest claims before payload streaming.
- Transitional Python runtime code is retired. The remaining `python/` package is the explicit ANALYSIS external adapter package.

## Install

```bash
uv sync --extra test
mkdir -p ~/.config/gmeow
cp gmeow.toml-example gmeow.toml
age-keygen -o ~/.config/gmeow/key.txt
chmod 600 ~/.config/gmeow/key.txt
```

Edit `gmeow.toml`. Phase 00 uses the Go config parser for all binaries. Startup requires a SOPS age identity from `GMEOW_SOPS_UNLOCK_KEY` or `~/.config/gmeow/key.txt`; config validation cannot be disabled. Keep operational TOML fields readable and store referenced `[secrets]` values as SOPS JSON leaf envelopes.

```toml
[system]
config_version = 1
instance_id = "local"
data_dir = "data"

[filestore]
root = "data/filestore"

[rpc.filestore]
network = "unix"
address = "/run/gmeow/filestore.sock"

[rpc.scheduler]
network = "unix"
address = "/run/gmeow/scheduler.sock"

[rpc.query]
network = "unix"
address = "/run/gmeow/query.sock"

[postgres]
host = "127.0.0.1"
port = 5432
database = "gmeow"
user = "gmeow"
ssl_mode = "require"

[rabbitmq]
host = "127.0.0.1"
port = 5672
user = "gmeow"
vhost = "gmeow"
test_user = "gmeow-test"
test_vhost = "gmeow-test"

[scheduler]
queue_prefix = "gmeow."

[secrets]
postgres_password = '''
{"data":"ENC[AES256_GCM,data:...leaf ciphertext...,type:str]","sops":{"age":[{"recipient":"age1...","enc":"-----BEGIN AGE ENCRYPTED FILE-----\n...\n-----END AGE ENCRYPTED FILE-----\n"}],"version":"3.13.1"}}
'''
rabbitmq_password = '''
{"data":"ENC[AES256_GCM,data:...leaf ciphertext...,type:str]","sops":{"age":[{"recipient":"age1...","enc":"-----BEGIN AGE ENCRYPTED FILE-----\n...\n-----END AGE ENCRYPTED FILE-----\n"}],"version":"3.13.1"}}
'''
rabbitmq_test_password = '''
{"data":"ENC[AES256_GCM,data:...leaf ciphertext...,type:str]","sops":{"age":[{"recipient":"age1...","enc":"-----BEGIN AGE ENCRYPTED FILE-----\n...\n-----END AGE ENCRYPTED FILE-----\n"}],"version":"3.13.1"}}
'''
```

Do not put `secrets.file`, `secrets.sops_file`, or `config.file` inside `gmeow.toml`. Config source selection belongs to `--config`, `GMEOW_CONFIG`, or the default `gmeow.toml`. Mandatory password leaves must be SOPS-encrypted; plaintext password leaves are rejected.

Gmail SOURCE behavior is implemented by the Go adapter. Gmail actions remain deliberately narrow:
apply/remove label, archive, mark read, and star. SOURCE actions are gated by adapter capability and
matching object provenance/facets.

## Run

```bash
go run ./cmd/gmeow-admin --config gmeow.toml config validate
go run ./cmd/gmeow --config gmeow.toml status
go run ./cmd/gmeow --config gmeow.toml filestore-serve
go run ./cmd/gmeow --config gmeow.toml query-serve
go run ./cmd/gmeow --config gmeow.toml scheduler-serve
go run ./cmd/gmeow --config gmeow.toml mcp-serve
go run ./cmd/gmeow --config gmeow.toml rest-serve
go run ./cmd/gmeow --config gmeow.toml imap-serve
```

Phase 01/02 development commands include `gmeow-admin filestore verify`,
`gmeow-admin query rebuild`, and `gmeow-admin query project-changed --since <RFC3339>` for
FILESTORE verification and QUERY projection work.
SCHEDULER provides `gmeow-admin scheduler scan`, `status`, `dead-letter`, `requeue`, and `force`;
RabbitMQ is mandatory. Production queues use the `gmeow.` prefix on the `gmeow` vhost; integration
tests use the `gmeow.test.` prefix on the `gmeow-test` vhost.

ANALYSIS workers are started with `go run ./cmd/gmeow-worker --config gmeow.toml analysis`.
Python/model analyzers must be configured as explicit external adapters, for example
`gmeow-intel analyze ner.spacy` or `gmeow-intel analyze categories.sklearn`; commandless Python
analyzers fail closed at startup.

The gRPC service protocol is typed protobuf. JSON is limited to dynamic metadata leaf fields, not
whole request or response envelopes. See `proto/gmeow/v1/` and `docs/systemd.md`.

## Distribution

Go binaries and future container images are released outside PyPI. The only planned PyPI package is
`python/` (`gmeow-intel`), which contains Python-native ANALYSIS workers and shared job/annotation
contract validation.

## Interfaces

MCP tools, REST endpoints, CLI workflows, and read-only IMAP all call the shared Go application
services. Protocol packages parse requests and shape responses; they do not import concrete
FILESTORE, QUERY, SOURCE, or SCHEDULER implementations.

## Local Data

By default, local data is ignored by git and stored under `data/`:

- `data/filestore/` is the Go FILESTORE root.
- PostgreSQL, RabbitMQ, object storage, query indexes, analysis annotations, and source state are
  runtime data.

## Archive and IMAP

IMAP is read-only and exposes only objects with the `mail_message` facet. Non-mail facets remain
invisible to IMAP clients.

## Development

The canonical local quality gate is one command:

```bash
make check
```

PostgreSQL-backed integration tests use the mandatory local `gmeow.toml` configuration and the
running local PostgreSQL service. Tests must clean only test-owned rows and must not create or drop
databases or schemas.

Before publishing, run the checklist in `docs/PUBLIC_RELEASE_CHECKLIST.md`.

## License

Gmeow is dual-licensed for open-source and proprietary/commercial use.

**Open Source License:** Gmeow is available under the GNU Affero General Public
License v3.0 only (AGPL-3.0-only). See `LICENSE`.

**Proprietary License:** Proprietary and commercial licenses are available by
separate written agreement. Contact <oss@blackcat.ca> to discuss commercial
terms.

<p align="center">
  <img src="https://raw.githubusercontent.com/paudley/gmeow/main/docs/gmeow-logo.svg" alt="Gmeow logo" width="280">
</p>

# Gmeow

Local Gmail intelligence for agents.

Gmeow turns a Gmail mailbox into a local intelligence layer for agents and automation. It exposes a loopback REST API and an MCP Streamable HTTP endpoint over an authenticated Gmail mailbox, while maintaining a local PostgreSQL-backed cache, semantic index, attachment object store, and knowledge graph.

Gmeow is designed for trusted single-user local systems. By default it binds to `127.0.0.1` and does not add application-level authentication. Do not expose it directly to an untrusted network.

## What Gmeow Does

- Gives agents a local MCP and REST interface to search, read, and act on Gmail without scraping a browser.
- Builds a durable local archive with raw RFC822 messages, attachment payloads, labels, threads, sync state, and search indexes.
- Adds semantic search, category discovery, and knowledge-graph views over mailbox content.
- Extracts attachment metadata and text so documents, images, archives, PDFs, and calendar files become searchable context.
- Serves the cached archive through read-only IMAP for tools that already speak mail protocols.

## Features

- Gmail mailbox access through Google Workspace service-account delegation or local user OAuth.
- REST and MCP tools for search, reads, labels, archive/read/star state, contacts, categories, graph exploration, and archive operations.
- PostgreSQL catalog for labels, threads, headers, MIME structure, categories, graph triples, jobs, sync state, and pgvector embedding chunks.
- Live Gmail search hydration: unbounded searches can query Gmail, cache returned messages, and enqueue analysis work.
- Full local object storage for payload bytes using a BLAKE3 content-addressed store with zstd compression for compressible content.
- Attachment sidecars with Gmail source metadata, `exiftool` metadata, extracted text, OCR, archive listings, document conversion, and optional vision captions.
- Semantic search using `semchunk` chunking and an OpenAI-compatible embedding endpoint.
- Knowledge graph extraction with RDF/RDFS, FOAF, SIOC, schema.org, SKOS, PROV-O, and DOAP alignment.
- Rustworkx graph projection, paths, ranking, centrality, components, project views, and related-node discovery.
- Category rules plus learned category suggestions from TF-IDF clustering.
- Timed maintenance jobs for history sync, priority sync, intelligence workers, derived views, PostgreSQL analyze, and optional sidecar refresh.
- Token-Oriented Object Notation by default for MCP responses, with JSON available on request.
- Read-only IMAP service backed by cached RFC822 archive objects.

## Install

```bash
uv sync --extra test
mkdir -p ~/.config/gmeow
cp gmeow.toml-example gmeow.toml
printf 'replace-with-local-unlock-key\n' > ~/.config/gmeow/key.txt
```

Edit `gmeow.toml`. Phase 00 uses the Go config parser for all binaries. Startup requires an unlock key from `GMEOW_SOPS_UNLOCK_KEY` or `~/.config/gmeow/key.txt`; config validation cannot be disabled.

```toml
[system]
config_version = 1
instance_id = "local"
data_dir = "data"

[filestore]
root = "data/filestore"

[postgres]
enabled = false
host = "127.0.0.1"
port = 5432
database = "gmeow"
user = "gmeow"
password_secret = ""
ssl_mode = "disable"

[secrets]
```

Do not put `secrets.file`, `secrets.sops_file`, or `config.file` inside `gmeow.toml`. Config source selection belongs to `--config`, `GMEOW_CONFIG`, or the default `gmeow.toml`. Any enabled component that references a secret requires the selected config file to be SOPS-protected; plaintext secret leaves are rejected.

Gmail provisioning and source runtime behavior move in later Go migration phases. Phase 00 only
validates foundation contracts and startup configuration.

## Run

```bash
go run ./cmd/gmeow-admin --config gmeow.toml config validate
go run ./cmd/gmeow --config gmeow.toml status
```

Phase 00 binaries validate config and report startup status without initializing unimplemented components.
Phase 01/02 development commands include `gmeow-admin filestore verify`,
`gmeow-admin query rebuild`, and `gmeow-admin query project-changed --since <RFC3339>` for
FILESTORE verification and QUERY projection work.

## Distribution

Go binaries and future container images are released outside PyPI. The only planned PyPI package is
`python/` (`gmeow-intel`), which contains Python-native ANALYSIS workers and shared job/annotation
contract validation.

## MCP

MCP returns in the Go INTERFACE phase. Phase 00 does not start MCP, REST, or IMAP services.

## Local Data

By default, local data is ignored by git and stored under `data/`:

- `data/filestore/` is the planned Go FILESTORE root.
- PostgreSQL, RabbitMQ, object storage, query indexes, and source state are wired in later phases.

## Archive and IMAP

Archive and IMAP behavior return in later Go phases. They are not operator-facing Phase 00 runtime
surfaces.

## Development

The canonical local quality gate is one command:

```bash
make check
```

PostgreSQL-backed integration tests are skipped unless `GMEOW_TEST_POSTGRES_DSN` points at a disposable test database. Tests must not use a production database.

Before publishing, run the checklist in `docs/PUBLIC_RELEASE_CHECKLIST.md`.

## License

Gmeow is dual-licensed for open-source and proprietary/commercial use.

**Open Source License:** Gmeow is available under the GNU Affero General Public
License v3.0 only (AGPL-3.0-only). See `LICENSE`.

**Proprietary License:** Proprietary and commercial licenses are available by
separate written agreement. Contact <oss@blackcat.ca> to discuss commercial
terms.

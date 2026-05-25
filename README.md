<p align="center">
  <img src="https://raw.githubusercontent.com/blackcat-informatics/gmeow/main/docs/gmeow-logo.svg" alt="Gmeow logo" width="280">
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
cp config.toml-example config.toml
```

Edit `config.toml`. All Gmeow settings live under `[gmeow]` so this file can be shared with other Google proxy/archive applications.

```toml
[gmeow]
subject = "user@example.com"
service_account_file = "data/secrets/service-account.json"
postgres_dsn = "postgresql://gmeow:change-me@127.0.0.1:5432/gmeow"
embedding_endpoint = "http://127.0.0.1:8090/v1/embeddings"

[gmeow.secrets]
file = "~/.config/gmeow/secrets.sops.yaml"
unlock_key = "replace-with-local-unlock-key"
age_key = "AGE-SECRET-KEY-REPLACE-WITH-LOCAL-SOPS-AGE-IDENTITY"
```

When `[gmeow.secrets]` is configured, Gmeow decrypts the SOPS YAML file at startup and uses `postgres_dsn`, `service_account_json`, `user_credentials_json`, and `imap_password` from that single encrypted file. Configured credential file paths remain explicit fallback paths for local deployments that do not use SOPS.

Generate Google Cloud service-account provisioning commands:

```bash
uv run gmeow provision-plan YOUR_PROJECT_ID
```

Authorize the generated service-account OAuth client ID in Google Admin Console for:

```text
https://www.googleapis.com/auth/gmail.modify
```

As a local fallback, set `auth_mode = "user_oauth"` under `[gmeow]` and create Gmail-scoped ADC credentials:

```bash
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/gmail.modify,https://www.googleapis.com/auth/cloud-platform
```

## Run

```bash
uv run gmeow serve --host 127.0.0.1 --port 8765
```

REST is available under `/api/v1`. MCP is mounted at `/mcp` when the installed MCP SDK provides an ASGI app.

Useful commands:

```bash
uv run gmeow status
uv run gmeow doctor
uv run gmeow sync
uv run gmeow sync-history
uv run gmeow refresh-attachment-sidecars
uv run gmeow enqueue-intelligence
uv run gmeow run-intelligence-worker
uv run gmeow rebuild-intelligence
uv run gmeow discover-categories --since-hours 48
uv run gmeow seed-categories
uv run gmeow recategorize
uv run gmeow category-stats
```

`gmeow serve` starts timed maintenance tasks from `[gmeow.maintenance]`. Use `GET /api/v1/maintenance/timed` for scheduler state and `POST /api/v1/maintenance/timed/{task_name}/run` to run one task immediately.

## MCP

Most MCP read/search tools return compact TOON by default. Pass `format: "json"` when an agent needs JSON-shaped results.

Example local agent configuration:

```json
{
  "mcpServers": {
    "gmeow": {
      "type": "http",
      "url": "http://127.0.0.1:8765/mcp"
    }
  }
}
```

Useful MCP tools include text/semantic/hybrid search, attachment text search, message/thread reads, attachment metadata, contacts/people, category tools, graph search/path/rank/project tools, status/help, history sync, priority sync, and limited mailbox actions.

## Local Data

By default, local data is ignored by git and stored under `data/`:

- PostgreSQL stores labels, threads, message headers/metadata, MIME structure, sync state, categories, graph triples, async jobs, and pgvector embedding chunks. Schema changes are versioned through Alembic migrations under `migrations/`.
- `data/objects/blake3/aa/bb/<digest>[.zst]` stores canonical payload bytes in the BLAKE3 content-addressed store.
- `data/objects/sidecars/aa/bb/<digest>.json` stores attachment sidecar metadata, source metadata, extracted text, and external enrichment.
- `data/tantivy/` stores the local lexical search index.
- `data/secrets/` stores local credential files only when you choose file-based credentials instead of SOPS.

## Archive and IMAP

Archive-complete messages require both Gmail full JSON and canonical raw RFC822 bytes. New Gmail hydrations fetch RFC822 automatically. Existing cached messages can be completed in bounded batches:

```bash
uv run gmeow complete-archive --limit 25
uv run gmeow archive-status
uv run gmeow verify-objects
```

Read-only IMAP is available as a loopback service. It exposes Gmail labels as folders, assigns stable per-folder UIDs, serves RFC822 from CAS, and rejects mutating IMAP commands.

```bash
printf 'choose-a-local-password\n' > data/secrets/imap-password
uv run gmeow serve-imap --host 127.0.0.1 --port 1143
```

## Development

```bash
uv sync --extra test
uv run python -m compileall main.py src migrations tests
uv run pytest
uv build
uv run python scripts/public_release_check.py
```

PostgreSQL-backed integration tests are skipped unless `GMEOW_TEST_POSTGRES_DSN` points at a disposable test database. Tests must not use a production database.

Before publishing, run the checklist in `docs/PUBLIC_RELEASE_CHECKLIST.md`.

## License

Gmeow is licensed under the MIT License. See `LICENSE`.

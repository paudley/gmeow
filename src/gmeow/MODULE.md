# Gmeow Source Package

The `gmeow` package implements the local Gmail intelligence server: durable PostgreSQL cache,
content-addressed attachment storage, semantic and lexical search, knowledge-graph projection,
and a Gmail mutation surface exposed over HTTP, MCP, IMAP, and CLI.

## Package Layout

- **Configuration** — [`config.py`](./config.py) loads `config.toml` and produces the typed
  `GmeowConfig` consumed everywhere.
- **Protocols** — [`protocols.py`](./protocols.py) hosts the shared `SyncCache`, `IntelligenceCache`,
  `GmeowCache`, `SyncSemantic`, `IntelligenceSemantic`, `GmeowSemantic`, `IntelligenceGraph`,
  `NoGraph`, and `SyncAttachments` structural types used across sync and intelligence.
- **Cache** — [`cache.py`](./cache.py), [`pg_cache.py`](./pg_cache.py),
  [`pg_cache_helpers.py`](./pg_cache_helpers.py),
  [`pg_cache_jobs.py`](./pg_cache_jobs.py),
  [`pg_cache_imap.py`](./pg_cache_imap.py),
  [`pg_cache_archive.py`](./pg_cache_archive.py).
- **Sync orchestration** — [`sync.py`](./sync.py) and
  [`sync_backfill.py`](./sync_backfill.py).
- **Gmail client** — [`gmail.py`](./gmail.py) plus shared mutations in
  [`gmail_actions.py`](./gmail_actions.py).
- **Object storage** — [`object_store.py`](./object_store.py) with derived analysis in
  [`attachment_analysis.py`](./attachment_analysis.py).
- **Intelligence and graph** — [`intelligence.py`](./intelligence.py),
  [`graph.py`](./graph.py), [`kg.py`](./kg.py),
  [`categories.py`](./categories.py), [`semantic_pg.py`](./semantic_pg.py),
  [`semantic.py`](./semantic.py), [`text_index.py`](./text_index.py).
- **Interfaces** — [`app.py`](./app.py) (FastAPI), [`mcp_server.py`](./mcp_server.py) (MCP),
  [`imap_server.py`](./imap_server.py) (read-only IMAP),
  [`cli.py`](./cli.py) (Typer).
- **Operations** — [`maintenance.py`](./maintenance.py),
  [`resilience.py`](./resilience.py), [`provision.py`](./provision.py),
  [`metadata.py`](./metadata.py), [`db.py`](./db.py), [`headers.py`](./headers.py),
  [`http_json.py`](./http_json.py), [`markdown.py`](./markdown.py),
  [`parser.py`](./parser.py), [`toon.py`](./toon.py), [`_typing.py`](./_typing.py).

## External Tool Prerequisites

- **PostgreSQL** with the `vector` (pgvector) and `age` (Apache AGE) extensions enabled.
- **Tesseract** for OCR on image attachments (optional, gated by
  `[gmeow.attachment_analysis] ocr_enabled`).
- **Pandoc** for `.docx` / `.odt` / `.rtf` extraction (optional, gated by
  `[gmeow.attachment_analysis] pandoc_enabled`).
- **Exiftool** for image and document metadata (optional, surfaces in
  `metadata.extract_exiftool_metadata`).
- **Nomic embedding endpoint** running locally at the URL in
  `[gmeow] embedding_endpoint` (default `http://127.0.0.1:8090/v1/embeddings`).

## Integration Test Prerequisites

Integration tests load the Postgres DSN from `config.toml` via
[`tests/_test_config.py`](../../tests/_test_config.py). The local config must contain:

```toml
[gmeow.testing]
postgres_dsn = "postgresql://gmeow-test:gmeow-test@127.0.0.1:5432/gmeow-test"
```

The DSN target must be a disposable database — the test suite truncates every table during setup.

## Operational Notes

- The Gmail mutation surface is intentionally narrow: only label, archive, mark-read, and star.
  Add new mutations through [`gmail_actions.py`](./gmail_actions.py) so the API and MCP surfaces
  stay aligned.
- All durable jobs run through [`pg_cache_jobs.py`](./pg_cache_jobs.py); do not introduce new
  background queues without going through that module.
- The compact MCP/HTTP response format is TOON; see [`toon.py`](./toon.py) for the encoding rules.
